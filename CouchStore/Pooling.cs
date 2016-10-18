using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CouchStore
{
	internal class ConcurrentWorkingQueue<T>
	{
		private BlockingCollection<T> _queue;

		public ConcurrentWorkingQueue(int boundedCapacity=0)
		{
			if (boundedCapacity > 0)  // Actually, I believe BlockingCollection(int) is already doing like this
			{
				this._queue = new BlockingCollection<T>(boundedCapacity);
			}
			else
			{
				this._queue = new BlockingCollection<T>();
			}
		}

		internal bool TryTake(out T entry)
		{
			return this._queue.TryTake(out entry);
		}
	}

	public class ConcurrentDispatcher<T> where T : class
	{
		public ConcurrentWorkingQueue<T> WorkingQueue = new ConcurrentWorkingQueue<T>();
		public int Index { get; set; }
		public int MilliSecondsForCleanUp { get { return 1000; } }

		private CancellationTokenSource _token_source = null;

		public void Start()
		{
			lock (this)
			{
				if (this._token_source != null)
				{
					throw new InvalidOperationException("Dispatcher is already running");
				}
				this._token_source = new CancellationTokenSource();

				CancellationToken token = _token_source.Token;
				Task.Factory.StartNew(async () => await this.DoDispatch(token), token);
			}
		}

		public void Stop()
		{
			lock (this)
			{
				if (this._token_source == null)
				{
					// Log : Already stopped
					return;
				}

				this._token_source.CancelAfter(this.MilliSecondsForCleanUp);
			}
		}

		protected async Task DoDispatch(CancellationToken ct)
		{
			SetupDispatch();

			try
			{
				T entry;
				while (ct.IsCancellationRequested == false)
				{
					if (this.WorkingQueue.TryTake(out entry))
					{
						this.Process(entry);
					}
					else
					{
						await Task.Delay(100);
					}
				}
			}
			finally
			{
				TeardownDispatch();
				// Wait until GC: _token_source.Dispose();

				lock (this)
				{
					if (this._token_source != null)
					{
						this._token_source.Dispose();
						this._token_source = null;
					}
				}
			}
		}
		
		protected virtual void SetupDispatch();
		protected virtual void TeardownDispatch();
		protected virtual bool Process(T entry);
	}

	public interface IDispatcherFactory<T> where T : class {
		ConcurrentDispatcher<T> CreateNew();
	}

	public class PooledDispatcherManager<T> where T : class {
		private ConcurrentWorkingQueue<T> _working_queue = new ConcurrentWorkingQueue<T>(-1);
		private List<ConcurrentDispatcher<T>> _pool = null;

		private IDispatcherFactory<T> _dispatcher_factory = null;

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
					for (int i = value; i < count; ++i)
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
						var new_dispatcher = this._dispatcher_factory.CreateNew();
						new_dispatcher.Index = i;
						new_dispatcher.WorkingQueue = this._working_queue;
						_pool.Add(new_dispatcher);
					}
				}
			}
		}

		public PooledDispatcherManager(IDispatcherFactory<T> factory, int workers = 1, int capacity = 0)
		{
			this._dispatcher_factory = factory;
			this.Workers = workers;
			this._working_queue = new ConcurrentWorkingQueue<T>(capacity);
		}

		public 
	}
}
