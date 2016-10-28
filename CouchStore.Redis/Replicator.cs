using CouchStore;
using ServiceStack.Redis;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CouchStore.Redis
{
	public class Replicator : IDisposable
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		private CancellationTokenSource _cancellation = null;
		private CouchStoreConfigManager<ReplicatorConfiguration> _config_manager = new CouchStoreConfigManager<ReplicatorConfiguration>();
		private List<KeyValuePair<string, RedisPubSubServer>> _subscribers = new List<KeyValuePair<string, RedisPubSubServer>>();
		private ConcurrentDictionary<string, OrderedPooledCouchConveyor<string>> _couch_stores = new ConcurrentDictionary<string, OrderedPooledCouchConveyor<string>>();

		public ReplicatorConfiguration Config { get; private set; }
		public PooledRedisClientManager _redis_pool = null;

		public Replicator()
		{
		}

		public void Dispose()
		{
			if (_cancellation != null && _cancellation.IsCancellationRequested == false)
			{
				_cancellation.Cancel();
			}

			_config_manager.Dispose();
			
			foreach (var s in _couch_stores)
			{
				s.Value.Dispose();
			}
			_couch_stores.Clear();
		}

		public void Initialize(string couchdb_host, string couchdb_name, string config_id = null)
		{
			Dispose();
			Logger.Info("Start to initialize");

			if (this._config_manager.LoadFrom(couchdb_host, couchdb_name, config_id))
			{
				this.Config = this._config_manager.Config;

				this._redis_pool = new PooledRedisClientManager(this.Config.RedisConnectionPoolSize, this.Config.RedisPoolTimeoutSeconds, this.Config.RedisServers);
				Logger.InfoFormat("Redis pool is initiailzed for {0} with size={1}, timeout={2}", this.Config.RedisServers, this.Config.RedisConnectionPoolSize, this.Config.RedisPoolTimeoutSeconds);

				foreach (var r in this.Config.HashReplications)
				{
					GetCouchStore(r.CouchTargetDatabase);
				}
				foreach (var s in this._couch_stores.Values)
				{
					var started = s.StartAll();
					Logger.InfoFormat("Total {0} workers are started to store for {1}", started, s.Name);
				}
			}
			else
			{
				throw new SystemException(string.Format("Can't load configuration from {0}/{1}/{2}", couchdb_host, couchdb_name, config_id));
			}
		}

		public OrderedPooledCouchConveyor<string> GetCouchStore(string dbname)
		{
			return _couch_stores.GetOrAdd(dbname, (key) =>
			{
				var write_scale = this.Config.WriteScalePerDatabase.GetValue(key, () => this.Config.WriteScaleDefault);
				return new OrderedPooledCouchConveyor<string>(this.Config.CouchDbEndpoint, key, write_scale);
			});
		}

		public RedisPubSubServer Subscribe(string channel)
		{
			var subscriber = new RedisPubSubServer(this._redis_pool, channel) { OnMessage = this.OnSubscribeMessage };
			subscriber.Start();

			Logger.InfoFormat("Subscribe Redis channel: {0}", channel);

			this._subscribers.Add(new KeyValuePair<string, RedisPubSubServer>(channel, subscriber));
			return subscriber;
		}

		public void OnSubscribeMessage(string key, string message)
		{
			Logger.DebugFormat("Message recevied [{0}]: {1}", key, message);
		}

		public bool CopyHashValue(string redis_hashname, string redis_hashkey, string couch_dbname)
		{
			using (var redis = _redis_pool.GetReadOnlyClient())
			{
				var store = GetCouchStore(couch_dbname);
				string value = redis.GetValueFromHash(redis_hashname, redis_hashkey);
				if (value != null)
				{
					store.Convey(redis_hashkey, value);
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		public int CopyHash(string redis_hashname, string couch_dbname)
		{
			using (var redis = _redis_pool.GetReadOnlyClient())
			{
				var store = GetCouchStore(couch_dbname);
				if (redis.GetHashCount(redis_hashname) > 0)
				{
					var dictionary = redis.GetAllEntriesFromHash(redis_hashname);
					foreach (var e in dictionary)
					{
						store.Convey(e.Key, e.Value);
					}
					Logger.DebugFormat("Total {0} entries of hash {1} is replicated to couchdb {2}", dictionary.Count, redis_hashname, couch_dbname);
					return dictionary.Count;
				}
				else
				{
					return 0;
				}
			}
		}

		public async Task<int> StartIntervalReplication(CancellationToken token)
		{
			Logger.InfoFormat("Start to replicate all hashes in every {0} seconds. Follow hash -> docs will be replicated.: {1}"
				, this.Config.HashReplicationIntervalSeconds
				, string.Join(", ", this.Config.HashReplications.Select((r) => string.Format("{0}->{1}", r.RedisSourceKey, r.CouchTargetDatabase))));

			int iteration = 1;
			var tasks = this.Config.HashReplications.Select((r) => Task.Run(() => CopyHash(r.RedisSourceKey, r.CouchTargetDatabase))).ToList();
			while (token.IsCancellationRequested == false)
			{
				if (this.Config.HashReplicationIntervalSeconds > 0)
				{
					await Task.Delay((int)(this.Config.HashReplicationIntervalSeconds * 1000));
					
					tasks = this.Config.HashReplications.Select((r) => Task.Run(() => CopyHash(r.RedisSourceKey, r.CouchTargetDatabase))).ToList();
					++iteration;
				}
				else
				{
					await Task.Delay(1000); // Just wait to check configuration until it is changed
				}

				foreach (var t in tasks.Where((t) => !t.IsCompleted))
				{
					Logger.WarnFormat("Previouse interval task {0} is not finished until next execution (IsFault={1}, IsCanceled={2}). Consider to extend interval time. It can be changed dynamically.",
						t.ToString(), t.IsFaulted, t.IsCanceled);
					t.Wait();
				}
			}

			Logger.InfoFormat("Total {0} times of Copy Task was executed", iteration);
			return iteration;
		}

		public CancellationTokenSource Start(string couchdb_host, string couchdb_name, string config_id = null)
		{
			Initialize(couchdb_host, couchdb_name, config_id);

			if (_cancellation != null)
			{
				if (_cancellation.IsCancellationRequested)
				{
					Logger.Info("Cancellation is in progress. Wait to finish.");
					SpinWait.SpinUntil(() => this._cancellation == null, 30000);
					if (_cancellation != null)
					{
						throw new InvalidOperationException("Cancellation is not finished in 30 secs.");
					}
				}
				else
				{
					throw new InvalidOperationException("Previous execution is not finished yet. Please Stop() it first.");
				}
			}
			_cancellation = new CancellationTokenSource();
			var ct = _cancellation.Token;

			Task.Factory.StartNew(async () => await this.StartIntervalReplication(_cancellation.Token));

			foreach (var ch in this.Config.ChannelsToSubscribe)
			{
				Subscribe(ch);
			}

			ct.Register(() => StopInternal());
			return _cancellation;
		}

		private void StopInternal() {
			if (_cancellation == null) {
				return;
			}

			if (_cancellation.IsCancellationRequested == false)
			{
				throw new InvalidOperationException("Working is not canceled. Please try use Stop() insteady of call StopInternal() directly");
			}

			foreach (var s in _subscribers)
			{
				s.Value.Dispose();
			}
			_subscribers.Clear();

			_cancellation.Dispose();
			_cancellation = null;
		}

		public bool Stop() 
		{
			if( _cancellation == null) {
				return false;
			}
			if (_cancellation.IsCancellationRequested) {
				return true;
			}

			_cancellation.Cancel();
			return true;
		}
	}
}