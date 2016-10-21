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

namespace CouchStore
{
	public class CouchStoreEventHandler<T> where T : class
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		private static CouchStoreEventHandler<T> _default_instance = new CouchStoreEventHandler<T>();
		public static CouchStoreEventHandler<T> Default { get { return _default_instance; } }

		public delegate void StoredEventHandler(string id, string rev, T entity);
		public delegate void FailedEventHandler(string id, T entity, Exception ex);

		public event StoredEventHandler OnStoredEvent;
		public event FailedEventHandler OnFailedEvent;

		public void OnStored(string id, string rev, T entity)
		{
			Logger.DebugFormat("Object is stored: id=[{0}], rev=[{1}], entity=[{2}]", id, rev, entity);
			OnStoredEvent(id, rev, entity);
		}
		public void OnFailed(string id, T entity, Exception ex)
		{
			Logger.DebugFormat("Failed to store object: id=[{0}], entity=[{1}], exception=[{2}]", id, entity, ex.Message);
			OnFailedEvent(id, entity, ex);
		}
	}

	public class InstantCouchStoreEventHandler<T> : CouchStoreEventHandler<T> where T : class
	{
		public InstantCouchStoreEventHandler(CouchStoreEventHandler<T>.StoredEventHandler on_stored = null, CouchStoreEventHandler<T>.FailedEventHandler on_failed = null)
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

	public class CouchStoreWaitEventHandler<T> : CouchStoreEventHandler<T> where T : class
	{
		private EventWaitHandle _wait_handle;
		public CouchStoreWaitEventHandler(EventWaitHandle wait_handle)
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
	
	public class DefaultCouchStoreEventHandler : CouchStoreEventHandler<object>
	{
		private static DefaultCouchStoreEventHandler _default_instance = new DefaultCouchStoreEventHandler();
		public static new DefaultCouchStoreEventHandler Default { get { return _default_instance; } }
		public DefaultCouchStoreEventHandler()
		{
			this.OnStoredEvent += delegate(string id, string rev, object entity) { };
			this.OnFailedEvent += delegate(string id, object entity, Exception ex) { };
		}
	}

	public interface CouchStoreEntry
	{
		string Id { get; }
	}

	public class CouchStoreEntry<T> : CouchStoreEntry where T : class 
	{
		public string Id { get; private set; }
		public int Hash { get; private set; }
		public T Entity { get; private set; }
		public string Json { get; private set; }
		public DateTime TimeStamp { get; private set; }
		public bool Overwrite { get; set; }
		public CouchStoreEventHandler<T> Handler { get; private set; }

		public CouchStoreEntry(string id, T entity, string json = null, bool overwrite = true, CouchStoreEventHandler<T> handler = null)
		{
			this.Id = id;
			this.Hash = id.GetHashCode();
			this.Entity = entity;
			this.Json = json;
			this.Overwrite = overwrite;
			this.Handler = handler ?? CouchStoreEventHandler<T>.Default;
			this.TimeStamp = DateTime.UtcNow;
		}
	}

	public class CouchStoreDispatcher<T> : ConcurrentDispatcher<CouchStoreEntry<T>> where T : class 
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		public MyCouchClient Client { get; set; }
		private DbConnectionInfo _connection_info = null;

		public CouchStoreDispatcher(DbConnectionInfo connection_info)
		{
			_connection_info = connection_info;
		}

		public async Task<DocumentHeaderResponse> TryPut(CouchStoreEntry<T> entry, string revision)
		{
			try
			{
				var header = await (revision == null ?
					this.Client.Documents.PutAsync(entry.Id, entry.Json) :
					this.Client.Documents.PutAsync(entry.Id, revision, entry.Json));
				if (header.StatusCode == System.Net.HttpStatusCode.Conflict)
				{
					throw new MyCouchResponseException(header);
				}
				return header;
			}
			catch (MyCouchResponseException ex)
			{
				Logger.DebugFormat("Conflict during put document {0}:{1} by {2}", entry.Id, revision, ex);
				if (entry.Overwrite)
				{
					return null;  // In this case, we will retry this outside so it's expected exception.
				}
				throw ex;
			}
		}

		private async Task<DocumentHeaderResponse> Store(CouchStoreEntry<T> entry, string revision)
		{
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
					res = await TryPut(entry, head.Rev);  // We still have a change to get conflict by timing
				}
			}
			return res;
		}

		protected override async Task<bool> Process(CouchStoreEntry<T> entry)
		{
			if (this.Client == null)
			{
				Logger.ErrorFormat("CouchDb client is not ready to process: {0}", entry);
				if (entry.Handler != null)
				{
					entry.Handler.OnFailed(entry.Id, entry.Entity, new InvalidOperationException("CouchDb client is not ready."));
				}
				return false;
			}

			try
			{
				var header = await Store(entry, null);
				if(header != null && header.IsSuccess) {
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
				this.Client = new MyCouchClient(_connection_info);
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

	public class CouchStoreDispatcherFactory<T> : IDispatcherFactory<CouchStoreEntry<T>> where T : class 
	{
		protected DbConnectionInfo _connection_info = null;

		public CouchStoreDispatcherFactory(string serverAddress, string dbName)
		{
			_connection_info = new DbConnectionInfo(serverAddress, dbName);
		}

		public override ConcurrentDispatcher<CouchStoreEntry<T>> CreateNew()
		{
			return new CouchStoreDispatcher<T>(_connection_info);
		}
	}

	public class PooledCouchStore<T> : PooledDispatcherManager<CouchStoreEntry<T>> where T : class 
	{
		public ISerializer Serializer { get; private set; }
		public MyCouchClientBootstrapper MyCouchClientBootstrapper { get; private set; }

		public PooledCouchStore(IDispatcherFactory<CouchStoreEntry<T>> factory, int workers = 1, int capacity = 0)
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

		public PooledCouchStore(string hostname, string dbname, int workers = 1, int capacity = 0)
			: this(new CouchStoreDispatcherFactory<T>(hostname, dbname), workers, capacity)
		{
		}

		public void Store(string id, T value, CouchStoreEventHandler<T> handler = null)
		{
			string json = this.Serializer.Serialize<T>(value);
			StringBuilder builder = new StringBuilder(json.Substring(0, json.LastIndexOf('}')));
			builder.AppendFormat(", {0}$timeStamp{0} : {1} {2}", '"', this.Serializer.ToJson(DateTime.UtcNow), '}');
			json = builder.ToString();
			// TODO: Add Timestamp to json
			this.AddEntry(new CouchStoreEntry<T>(id, value, json, true, handler), -1); // find best method to call this. Current method AddEntry can block
		}
	}

	public class CouchTaskSerializeContext
	{
		public string Id { get; set; }
		public string Rev { get; set; }
		public ConcurrentQueue<CouchStoreEntry> WaitingQueue { get; private set; }

		public CouchTaskSerializeContext(string id)
		{
			if (String.IsNullOrEmpty(id))
			{
				throw new ArgumentNullException("id");
			}
			this.Id = id;
			this.WaitingQueue = new ConcurrentQueue<CouchStoreEntry>();
		}
	}

	public class CouchTaskSerializer
	{
		private ConcurrentDictionary<string, CouchTaskSerializeContext> _working_map = new ConcurrentDictionary<string, CouchTaskSerializeContext>();
	}

	public class CouchStoreSerializedDispatcher<T> : CouchStoreDispatcher<T> where T : class 
	{
		private CouchTaskSerializer _task_serializer;
		public CouchStoreSerializedDispatcher(DbConnectionInfo connection_info, CouchTaskSerializer task_serializer)
			: base(connection_info)
		{
			if (connection_info == null)
			{
				throw new ArgumentNullException("connection_info");
			}
			if (task_serializer == null)
			{
				throw new ArgumentNullException("task_serializer");
			}
			_task_serializer = task_serializer;
		}
	}

	public class CouchStoreSerializedDispatcherFactory<T> : CouchStoreDispatcherFactory<T> where T : class 
	{
		private CouchTaskSerializer _task_serializer = new CouchTaskSerializer();

		public CouchStoreSerializedDispatcherFactory(string serverAddress, string dbName)
			: base(serverAddress, dbName)
		{
		}

		public override ConcurrentDispatcher<CouchStoreEntry<T>> CreateNew()
		{
			return new CouchStoreSerializedDispatcher<T>(_connection_info, _task_serializer);
		}
	}

	public class SerializedPooledCouchStore<T> : PooledCouchStore<T> where T : class 
	{
		public SerializedPooledCouchStore(string hostname, string dbname, int workers = 1, int capacity = 0)
			: base(new CouchStoreSerializedDispatcherFactory<T>(hostname, dbname), workers, capacity)
		{
		}
	}
}
