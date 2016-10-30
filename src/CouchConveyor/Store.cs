using MyCouch;
using MyCouch.Responses;
using MyCouch.Serialization;
using MyCouch.Serialization.Meta;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CouchConveyor
{
	public class CouchConveyorEventHandler<T> where T : class
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		private static CouchConveyorEventHandler<T> _default_instance = new CouchConveyorEventHandler<T>();
		public static CouchConveyorEventHandler<T> Default { get { return _default_instance; } }

		public delegate void StoredEventHandler(string id, string rev, T entity);
		public delegate void FailedEventHandler(string id, T entity, Exception ex);

		public event StoredEventHandler OnStoredEvent;
		public event FailedEventHandler OnFailedEvent;

		public void OnStored(string id, string rev, T entity)
		{
			Logger.DebugFormat("Object is stored: id=[{0}], rev=[{1}], entity=[{2}]", id, rev, entity);
			try
			{
				if (OnStoredEvent != null)
				{
					OnStoredEvent(id, rev, entity);
				}
			}
			catch (Exception ex)
			{
				Logger.Warn("Handler throws an exception on handling stored event", ex);
			}
		}
		public void OnFailed(string id, T entity, Exception reason)
		{
			Logger.DebugFormat("Failed to store object: id=[{0}], entity=[{1}], exception=[{2}]", id, entity, reason.Message);
			try
			{
				if (OnFailedEvent != null)
				{
					OnFailedEvent(id, entity, reason);
				}
			}
			catch (Exception ex)
			{
				Logger.Warn("Handler throws an exception on handling failed event", ex);
			}
		}
	}

	public class InstantCouchConveyorEventHandler<T> : CouchConveyorEventHandler<T> where T : class
	{
		public InstantCouchConveyorEventHandler(CouchConveyorEventHandler<T>.StoredEventHandler on_stored = null, CouchConveyorEventHandler<T>.FailedEventHandler on_failed = null)
		{
			if (on_stored != null)
			{
				this.OnStoredEvent += on_stored;
			}
			if (on_failed != null)
			{
				this.OnFailedEvent += on_failed;
			}
		}
	}

	public class CouchConveyorWaitEventHandler<T> : CouchConveyorEventHandler<T> where T : class
	{
		private EventWaitHandle _wait_handle;
		public CouchConveyorWaitEventHandler(EventWaitHandle wait_handle)
		{
			_wait_handle = wait_handle;
			this.OnStoredEvent += delegate(string id, string revision, T entry)
			{
				_wait_handle.Set();
			};
			this.OnFailedEvent += delegate(string id, T entry, Exception ex)
			{
				_wait_handle.Set();
			};
		}
	}

	public class DefaultCouchConveyorEventHandler : CouchConveyorEventHandler<object>
	{
		private static DefaultCouchConveyorEventHandler _default_instance = new DefaultCouchConveyorEventHandler();
		public static new DefaultCouchConveyorEventHandler Default { get { return _default_instance; } }
		public DefaultCouchConveyorEventHandler()
		{
			this.OnStoredEvent += delegate(string id, string rev, object entity) { };
			this.OnFailedEvent += delegate(string id, object entity, Exception ex) { };
		}
	}

	public interface CouchConveyorEntry
	{
		string Id { get; }
	}

	public class CouchConveyEntry<T> : CouchConveyorEntry where T : class
	{
		public string Id { get; private set; }
		public int Hash { get; private set; }
		public T Entity { get; private set; }
		public string Json { get; private set; }
		public DateTime TimeStamp { get; private set; }
		public bool Overwrite { get; set; }
		public CouchConveyorEventHandler<T> Handler { get; private set; }

		public CouchConveyEntry(string id, T entity, string json = null, bool overwrite = true, CouchConveyorEventHandler<T> handler = null)
		{
			this.Id = id;
			this.Hash = id.GetHashCode();
			this.Entity = entity;
			this.Json = json;
			this.Overwrite = overwrite;
			this.Handler = handler ?? CouchConveyorEventHandler<T>.Default;
			this.TimeStamp = DateTime.UtcNow;
		}
	}

	public class CouchConveyorDispatcher<T> : ConcurrentDispatcher<CouchConveyEntry<T>> where T : class
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		public MyCouchClient Client { get; set; }
		private DbConnectionInfo _connection_info = null;

		public CouchConveyorDispatcher(DbConnectionInfo connection_info)
		{
			_connection_info = connection_info;
		}

		public async Task<DocumentHeaderResponse> TryPut(CouchConveyEntry<T> entry, string revision)
		{
			try
			{
				var header = await (revision == null ?
					this.Client.Documents.PutAsync(entry.Id, entry.Json) :
					this.Client.Documents.PutAsync(entry.Id, revision, entry.Json));
				if (header.StatusCode == System.Net.HttpStatusCode.Conflict && entry.Overwrite)
				{
					return null;  // In this case, we will retry this outside so it's expected exception.
				}
				return header;
			}
			catch (MyCouchResponseException ex)
			{
				Logger.DebugFormat("Error during put document {0}:{1} by {2}", entry.Id, revision, ex);
				if (ex.HttpStatus == System.Net.HttpStatusCode.Conflict && entry.Overwrite)
				{
					return null;  // In this case, we will retry this outside so it's expected exception.
				}
				throw ex;
			}
		}

		public async Task<DocumentHeaderResponse> Store(CouchConveyEntry<T> entry, string revision)
		{
			if (this.Client == null)
			{
				Logger.ErrorFormat("CouchDb client is not ready to process: {0}", entry);
				throw new InvalidOperationException("CouchDb client is not ready.");
			}

			var res = await TryPut(entry, revision);
			if (res != null)
			{
				return res;
			}

			if (entry.Overwrite)
			{
				while (res == null)
				{
					var head = await this.Client.Documents.HeadAsync(entry.Id);
					res = await TryPut(entry, head.Rev);  // We still have a chance to get conflict by timing
				}
			}
			return res;
		}

		protected override async Task<bool> Process(CouchConveyEntry<T> entry)
		{
			try
			{
				var header = await Store(entry, null);
				if (header != null && header.IsSuccess)
				{
					entry.Handler.OnStored(entry.Id, header.Rev, entry.Entity);
					return true;
				}

				throw new MyCouchResponseException(header);
			}
			catch (Exception ex)
			{
				entry.Handler.OnFailed(entry.Id, entry.Entity, ex);
				return false;
			}
		}

		protected override bool SetupDispatch()
		{
			if (this.Client == null)
			{
				this.Client = new MyCouchClient(this._connection_info);

				var task = this.Client.Database.GetAsync();
				task.Wait();
				if (false == task.Result.IsSuccess)
				{
					if (task.Result.StatusCode == System.Net.HttpStatusCode.Conflict)
					{
						Logger.WarnFormat("Some other dispatcher already made db {0} by {1}", this._connection_info, task.Result);
					}
					else if (task.Result.StatusCode == System.Net.HttpStatusCode.NotFound)
					{
						var put_task = this.Client.Database.PutAsync();
						put_task.Wait();
						if (false == put_task.Result.IsSuccess)
						{
							Logger.ErrorFormat("Can't connect or create CouchDB {0} by {1}", this._connection_info, put_task.Result);
							return false;
						}
					}
					else
					{
						return false;
					}
				}
			}
			return true;
		}

		protected override void TeardownDispatch()
		{
			if (this.Client != null)
			{
				this.Client.Dispose();
				this.Client = null;
			}
		}
	}

	public class CouchConveyorDispatcherFactory<T> : IDispatcherFactory<CouchConveyEntry<T>> where T : class
	{
		protected DbConnectionInfo _connection_info = null;

		public CouchConveyorDispatcherFactory(string serverAddress, string dbName)
		{
			_connection_info = new DbConnectionInfo(serverAddress, dbName);
		}

		public override ConcurrentDispatcher<CouchConveyEntry<T>> CreateNew()
		{
			return new CouchConveyorDispatcher<T>(_connection_info);
		}
	}

	public class PooledCouchConveyor<T> : PooledDispatcherManager<CouchConveyEntry<T>> where T : class
	{
		public ISerializer Serializer { get; private set; }
		public MyCouchClientBootstrapper MyCouchClientBootstrapper { get; private set; }

		public KeyValuePair<string, string> KeyValue<V>(string key, V value)
		{
			return new KeyValuePair<string, string>(key, this.Serializer.ToJson(value));
		}

		public PooledCouchConveyor(IDispatcherFactory<CouchConveyEntry<T>> factory, int workers = 1, int capacity = 0)
			: base(factory, workers, capacity)
		{
			if (factory == null)
			{
				throw new ArgumentNullException("factory");
			}

			// TODO: Extract this if we need to customize MyCouch
			this.MyCouchClientBootstrapper = new MyCouchClientBootstrapper();
			this.Serializer = this.MyCouchClientBootstrapper.SerializerFn();
		}

		public PooledCouchConveyor(string hostname, string dbname, int workers = 1, int capacity = 0)
			: this(new CouchConveyorDispatcherFactory<T>(hostname, dbname), workers, capacity)
		{
			this.Name = string.Format("{0}/{1}", hostname, dbname);
		}

		public string Serialize(T entity)
		{
			string json = (entity is string) ? entity as string : this.Serializer.Serialize<T>(entity);
			return InjectVariablesIntoJson(json, KeyValue("$timestamp", DateTime.UtcNow));
		}

		// TODO: Implement custom serializer and do this on it
		public string InjectVariablesIntoJson(string source, params KeyValuePair<string, string>[] properties)
		{
			StringBuilder builder = new StringBuilder();
			foreach (var p in properties)
			{
				builder.AppendFormat(", \"{0}\":{1}", p.Key, p.Value);
			}

			return source.Insert(source.LastIndexOf('}'), builder.ToString());
		}

		public void Convey(string id, T value, CouchConveyorEventHandler<T> handler = null)
		{
			string json = Serialize(value); // Serialize on synchronized context to store 'as-is' status
			this.AddEntry(new CouchConveyEntry<T>(id, value, json, true, handler), -1); // find best method to call this. Current method AddEntry can block
		}
	}
}
