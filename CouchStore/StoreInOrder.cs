using MyCouch;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CouchStore
{
	public class CouchTaskSerializeContext<T> where T : class
	{
		public string Id { get; set; }
		public string Rev { get; set; }
		public ConcurrentQueue<CouchStoreEntry<T>> WaitingQueue { get; private set; }
		public ConcurrentQueue<CouchStoreEntry<T>> ProcessedQueue { get; private set; }
		public ConcurrentQueue<Tuple<int, int>> ProcessedContext { get; private set; }
		public ConcurrentQueue<Tuple<int, int>> LockRace { get; private set; }

		private int _lock_id;
		private int _lock_seq;

		public CouchTaskSerializeContext(string id)
		{
			if (String.IsNullOrEmpty(id))
			{
				throw new ArgumentNullException("id");
			}
			this.Id = id;
			this.WaitingQueue = new ConcurrentQueue<CouchStoreEntry<T>>();
			this.ProcessedQueue = new ConcurrentQueue<CouchStoreEntry<T>>();
			this.ProcessedContext = new ConcurrentQueue<Tuple<int, int>>();
			this.LockRace = new ConcurrentQueue<Tuple<int, int>>();
			this._lock_seq = this._lock_id = 0;
		}

		internal void EndWork(int lock_id)
		{
			if (this._lock_id != lock_id)
			{
				throw new InvalidOperationException(string.Format("Invalid lock id {0} for current lock {1}", lock_id, this._lock_id));
			}
			long original = Interlocked.CompareExchange(ref this._lock_id, 0, lock_id);
			if (original != lock_id)
			{
				throw new InvalidOperationException(string.Format("Failed to clear current work {0}. The other context {1} has interfered.", this._lock_id, original));
			}
		}

		internal int? TryStartWork()
		{
			if (_lock_id != 0)
			{
				return null;
			}
			int lock_seq = Interlocked.Increment(ref this._lock_seq);
			if (lock_seq == 0)
			{ // In case of overflow
				lock_seq = Interlocked.Increment(ref this._lock_seq);
			}

			int original = Interlocked.CompareExchange(ref this._lock_id, lock_seq, 0);
			LockRace.Enqueue(Tuple.Create(this._lock_seq, original));
			return (original == 0 && this._lock_id == lock_seq) ? (int?)lock_seq : null;
		}

		internal void Processed(CouchStoreEntry<T> t)
		{
			ProcessedQueue.Enqueue(t);
			ProcessedContext.Enqueue(Tuple.Create(this._lock_seq, this._lock_id));
		}
	}

	public class CouchTaskSerializer<T> where T : class
	{
		private ConcurrentDictionary<string, CouchTaskSerializeContext<T>> _working_map = new ConcurrentDictionary<string, CouchTaskSerializeContext<T>>();

		public CouchTaskSerializeContext<T> GetContextFor(CouchStoreEntry<T> entry)
		{
			CouchTaskSerializeContext<T> context;
			if (_working_map.TryGetValue(entry.Id, out context))
			{
				return context;
			}

			return _working_map.GetOrAdd(entry.Id, new CouchTaskSerializeContext<T>(entry.Id));
		}
	}

	public class CouchStoreSerializedDispatcher<T> : CouchStoreDispatcher<T> where T : class
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		private CouchTaskSerializer<T> _task_serializer;
		public CouchStoreSerializedDispatcher(DbConnectionInfo connection_info, CouchTaskSerializer<T> task_serializer)
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

		// FIXME: This weird method extraction is for resolve synchronization problem between `this.WorkingQueue.TryTake(out entry)`
		//        and `context.WaitingQueue.Enqueue(entry);`. But this is too rough. Let me polish this later.
		protected override async Task<bool> TakeOneAndProcess()
		{
			CouchStoreEntry<T> entry = null;
			CouchTaskSerializeContext<T> context = null;
			lock (this._task_serializer)
			{
				if (this.WorkingQueue.TryTake(out entry))
				{
					context = _task_serializer.GetContextFor(entry);
					context.WaitingQueue.Enqueue(entry);
				}
				else
				{
					return false;
				}
			}

			if (entry == null || context == null)
			{
				Logger.Error("Invalid queue handling", new InvalidOperationException());
				return false;
			}

			int? lock_id = context.TryStartWork();
			if (null == lock_id)
			{
				return true;
			}
			try
			{
				CouchStoreEntry<T> t;
				while (context.WaitingQueue.TryDequeue(out t))
				{
					context.Processed(t);
					try
					{
						var header = await Store(t, context.Rev);
						if (header != null && header.IsSuccess)
						{
							t.Handler.OnStored(t.Id, header.Rev, t.Entity);
							context.Rev = header.Rev;
						}
						else
						{
							throw new MyCouchResponseException(header);
						}
					}
					catch (Exception ex)
					{
						t.Handler.OnFailed(t.Id, t.Entity, ex);
					}
				}
			}
			catch (Exception ex)
			{
				Logger.Error(new object[] { "Dispatcher throws uncaught exception.", entry }, ex);
			}
			finally
			{
				context.EndWork(lock_id ?? 0);
			}
			return true;
		}
	}

	public class CouchStoreSerializedDispatcherFactory<T> : CouchStoreDispatcherFactory<T> where T : class
	{
		private CouchTaskSerializer<T> _task_serializer = new CouchTaskSerializer<T>();

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