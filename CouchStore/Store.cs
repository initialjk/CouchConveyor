using MyCouch;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CouchStore
{
	public class CouchStoreEntry {
		public string Id { get; private set; }
		public int Hash { get; private set; }
		public object Entity { get; private set; }

		public CouchStoreEntry(string id, object entity) {
			this.Id = id;
			this.Hash = id.GetHashCode();
			this.Entity = entity;
		}	
	}
		
	public class CouchStoreTypedEntry<T> : CouchStoreEntry {
		public T TypedEntity { get; private set; }
		public CouchStoreTypedEntry(string id, T entity) : base(id, entity) {
			TypedEntity = entity;
		}	
	}

	public class CouchStoreWorkingQueue : IDisposable {	
		private BlockingCollection<CouchStoreEntry> _working_queue = null;
		public bool Active { get; set; }
	
		public CouchStoreWorkingQueue() {
		Active = true;
		}

		public bool TryTake(out CouchStoreEntry entry)
		{
 			while(Active) {
				if(_working_queue.TryTake(entry)) {
				return true;
				}
				if (Active) {
				await Task.Delay(100);
				}
			}			
		}

		public void Dispose() {
			this.Active = false;
		}
	}

	public class CouchStoreDispatcher
	{
		public MyCouchClient Client { get; set; }
		private DbConnectionInfo _connection_info = null;
		private CouchStoreWorkingQueue _working_queue = new CouchStoreWorkingQueue();
		private bool _working = false;
		public int Index { get; private set; }

		public CouchStoreDispatcher(DbConnectionInfo connection_info, CouchStoreWorkingQueue working_queue, int index) {
			this._connection_info = connection_info;
			this._working_queue = working_queue;
			this.Index = index;
		}

		public void Start() {
			this._working = true;
		}

		public void Stop() {
			this._working = false;
		}

		protected void Dispatch() {
			if (this.Client == null)
			{
				this.Client = new MyCouchClient(this._connection_info);
			}

			CouchStoreEntry entry;
			while (_working_queue.TryTake(out entry))
			{
				
			}

			if (this.Client != null) {
				this.Client.Dispose();
				this.Client = null;
			}
		}
		
		public void PostStore(string id, object entity, Action on_success, Action on_failed)
		{
		}
	}

	public interface ICouchStoreCallback { 
		public void OnStored(string id, string rev, object entity);
		public void OnFailed(string id, object entity, Exception ex);
	}

	public interface ICouchStore : IDisposable
	{
		public void PostStore(string id, object entity, ICouchStoreCallback callback = null);
	}

	public class PooledCouchStore {
		public DbConnectionInfo _connection_info;
		public List<CouchStoreDispatcher> _pool = null;
		public BlockingCollection<CouchStoreEntry> _working_queue = new BlockingCollection<CouchStoreEntry>();

		public int Workers
		{
			get
			{
				return _pool.Count;
			}
			set
			{
				int count = _pool.Count;
				if (count > value)
				{
					for (int i = value; i < value; ++i)
					{
						_pool[i].Stop();
					}
					_pool.RemoveRange(value, count - value);
				}
				else if (count < value)
				{
					_pool.Capacity = value;
					for (int i = count; i < value; ++i)
					{
						_pool.Add(new CouchStoreDispatcher(DbConnectionInfo connection_info, BlockingCollection<CouchStoreEntry> working_queue, int index);
					}
				}
				this._workers = value;
			}
		}

		public PooledCouchStore(DbConnectionInfo connection_info, int workers = 1, int capacity = 0)
		{
			this._connection_info = connection_info;
			this.Workers = workers;
			if (capacity > 0)  // Actually, I believe BlockingCollection(int) is already doing like this
			{
				_working_queue = new BlockingCollection<CouchStoreEntry>(capacity);
			}
			else
			{
				_working_queue = new BlockingCollection<CouchStoreEntry>();
			}
		}

		public void PostStore(string id, object entity, Action on_success, Action on_failed) {
		
		}
	}

	/// <summary>
	/// Serialize 
	/// </summary>
	/// <typeparam name="T">Entity type</typeparam>
	public class SerializedCouchStore<T> : ICouchStoreCallback where T : class
    {
		public delegate void StoredEventHandler(string id, string rev, T entity);
		public delegate void FailedEventHandler(string id, T entity, Exception ex);

		public event StoredEventHandler OnStored;
		public event FailedEventHandler OnFailed;

		public ICouchStore CouchStore { get; set; }
		public SerializedCouchStore(ICouchStore store, int capacity = 0)
		{
			this.CouchStore = store;
		}
		
		public void Store(string id, T entity) {
			if (this.CouchStore == null) {
				this.OnFailed(id, entity, new InvalidOperationException("Try to store entry to not configured repository"));
			}
		}
    }
}
