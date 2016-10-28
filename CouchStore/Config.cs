using MyCouch;
using MyCouch.Requests;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CouchStore
{
	public abstract class IConfiguration<T>
	{
		public string Id { get; protected set; }

		public abstract bool PreLoad();
		public abstract bool PreChange(T value, string[] properties);

		public delegate void OnLoadEventHandler();
		public delegate void OnChangedEventHandler(string[] properties);

		public event OnLoadEventHandler OnLoadEvent;
		public event OnChangedEventHandler OnChangedEvent;

		public void OnLoad()
		{
			if (this.OnLoadEvent != null)
			{
				this.OnLoadEvent();
			}
		}
		public void OnChanged(string[] properties)
		{
			if (OnChangedEvent != null)
			{
				this.OnChangedEvent(properties);
			}
		}
	}

	public class CouchStoreConfigManager<T> : IDisposable where T : IConfiguration<T>
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		private MyCouchClient _couch_client;
		private CancellationTokenSource _cancellation;
		public T Config { get; private set; }

		public CouchStoreConfigManager(T instance = null)
		{
			if (instance != null)
			{
				this.Config = instance;
			}
			else
			{
				var constructor = typeof(T).GetConstructor(new Type[] { });
				if (constructor != null)
				{
					this.Config = (T)constructor.Invoke(new object[] { });
				}
			}
		}

		public void Dispose()
		{
			if (_cancellation != null)
			{
				_cancellation.Cancel(false);
				_cancellation.Dispose();
				_cancellation = null;
			}

			if (_couch_client != null)
			{
				_couch_client.Dispose();
				_couch_client = null;
			}
		}

		internal void FeedData(string data)
		{
		}

		public bool LoadFrom(string couchdb_host, string db_name, string config_name = null, bool track_changes = false)
		{
			using (var couch_store = new MyCouchStore(couchdb_host, db_name))
			{
				config_name = config_name ?? ((this.Config != null) ? this.Config.Id : typeof(T).Name);

				var task = couch_store.GetByIdAsync<T>(config_name);
				task.Wait();
				if (task.Result != null)
				{
					if (task.Result.PreLoad())
					{
						task.Result.OnLoad();
						this.Config = task.Result;
						Logger.InfoFormat("Configuration is loaded from {0}/{1}/{2} successfully:\n\r{3}",
							couchdb_host, db_name, config_name, couch_store.Client.Serializer.Serialize(task.Result));
					}
					else
					{
						Logger.InfoFormat("Failed to load configuration from {0}/{1}/{2}", couchdb_host, db_name, config_name);
						return false;
					}
				}
				else
				{
					if (this.Config != null)
					{
						try
						{
							var write_task = couch_store.StoreAsync<T>(this.Config);
							write_task.Wait();
							Logger.InfoFormat("Configuration does not exist on {0}/{1}/{2}. So wrote back {3}:\n\r{4}",
								couchdb_host, db_name, config_name, write_task.Result, couch_store.Client.Serializer.Serialize(this.Config));
						}
						catch (Exception ex)
						{
							Logger.Warn("Failed to store back defaut configuration", ex);
							return false;
						}
					}
				}
			}

			if (false == track_changes)
			{
				return true;
			}

			throw new NotImplementedException("configuration tracking is not available now");

			/*
			var getChangesRequest = new GetChangesRequest
			{
				Feed = ChangesFeed.Continuous,
				Heartbeat = 1000,
			};

			_couch_client = new MyCouchClient(couchdb_host, db_name);
			_cancellation = new CancellationTokenSource();
			_couch_client.Changes.GetAsync(getChangesRequest, data => this.FeedData(data), _cancellation.Token);
			
			return true;
			 */
		}
	}
}
