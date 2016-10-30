using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CouchConveyor
{
	internal class ConcurrentWorkingQueue<T>
	{
		private BlockingCollection<T> _queue;

		public ConcurrentWorkingQueue(int boundedCapacity = 0)
		{
			if (boundedCapacity > 0)  // Actually, I believe BlockingCollection(int) is already doing like this
			{
				_queue = new BlockingCollection<T>(boundedCapacity);
			}
			else
			{
				_queue = new BlockingCollection<T>();
			}
		}

		public bool TryAdd(T entry)
		{
			return _queue.TryAdd(entry);
		}

		public bool BlockableAdd(T entry, int timeout_ms)
		{
			if (timeout_ms == 0)
			{
				return _queue.TryAdd(entry);
			}
			else if (timeout_ms > 0)
			{
				return _queue.TryAdd(entry, timeout_ms);
			}
			else
			{
				_queue.Add(entry);
				return true;
			}
		}

		internal bool TryTake(out T entry)
		{
			return _queue.TryTake(out entry);
		}

		internal T Take()
		{
			return _queue.Take();
		}
	}

	public abstract class ConcurrentDispatcher<T> where T : class
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		internal ConcurrentWorkingQueue<T> WorkingQueue = new ConcurrentWorkingQueue<T>();
		public int Index { get; set; }
		public Task Task { get; private set; }

		private CancellationTokenSource _token_source = null;

		public bool Start(CancellationToken sct)
		{
			lock (this)
			{
				if (_token_source != null)
				{
					Logger.WarnFormat("Tried to start Dispatcher which is already started. Index={0}", this.Index);
					return false;
				}
				_token_source = CancellationTokenSource.CreateLinkedTokenSource(sct);

				CancellationToken token = _token_source.Token;
				token.Register(() => this.Clear());

				if (false == SetupDispatch())
				{
					Logger.ErrorFormat("Failed to setup dispatcher {0}", this.Index);
					throw new OperationCanceledException("Failed to setup dispatcher");
				}

				if (Index % 2 == 0)
				{
					this.Task = Task.Factory.StartNew(async (obj) => await this.DoDispatch(token), token, TaskCreationOptions.LongRunning);
				}
				else
				{
					this.Task = Task.Run(async () => await this.DoDispatch(token), token);
				}
			}
			return true;
		}

		public bool Stop()
		{
			lock (this)
			{
				if (_token_source == null)
				{
					Logger.WarnFormat("Tried to stop Dispatcher which is already stopped. Index={0}", this.Index);
					return false;
				}
				if (_token_source.IsCancellationRequested)
				{
					Logger.InfoFormat("Dispatcher is already on cancellation. Index={0}", this.Index);
					return false;
				}
				else
				{
					_token_source.Cancel();
				}

				var task = this.Task;
				if (task != null)
				{
					this.Task.Wait();
				}

				Clear();
			}
			return true;
		}

		private void Clear()
		{
			if (this._token_source != null)
			{
				this._token_source.Dispose();
			}
			this._token_source = null;
			this.Task = null;
		}

		protected async Task DoDispatch(CancellationToken ct)
		{
			try
			{
				Logger.DebugFormat("Start to dispatch entries at {0}", this.Index);
				int processed = 0;
				while (ct.IsCancellationRequested == false)
				{
					if (await TakeOneAndProcess())
					{
						++processed;
					}
					else
					{
						await Task.Delay(100);  // If we didn't get an instance, take a brake.
					}
				}
				Logger.DebugFormat("Finish to dispatch entries at {0}, {1} entries are processed.", this.Index, processed);
			}
			finally
			{
				TeardownDispatch();

				lock (this)
				{
					Clear();
				}
			}
		}

		protected virtual async Task<bool> TakeOneAndProcess()
		{
			T entry = null;
			if (this.WorkingQueue.TryTake(out entry))
			{
				try
				{
					await this.Process(entry);
				}
				catch (Exception ex)
				{
					Logger.Error(new object[] { "Dispatcher throws uncaught exception.", entry }, ex);
				}
				return true;
			}
			return false;
		}

		protected abstract bool SetupDispatch();
		protected abstract void TeardownDispatch();
		protected abstract Task<bool> Process(T entry);
	}

	public abstract class IDispatcherFactory<T> where T : class
	{
		public abstract ConcurrentDispatcher<T> CreateNew();
	}

	public class PooledDispatcherManager<T> : IDisposable where T : class
	{
		public string Name { get; protected set; }

		private ConcurrentWorkingQueue<T> _working_queue = new ConcurrentWorkingQueue<T>(0);
		private List<ConcurrentDispatcher<T>> _pool = new List<ConcurrentDispatcher<T>>();
		private CancellationTokenSource _cancellation = new CancellationTokenSource();

		private IDispatcherFactory<T> _dispatcher_factory = null;

		public PooledDispatcherManager(IDispatcherFactory<T> factory, int workers = 1, int capacity = 0)
		{
			_dispatcher_factory = factory;
			_working_queue = new ConcurrentWorkingQueue<T>(capacity);
			this.Workers = workers;
		}

		public int StartAll()
		{
			lock (_pool)
			{
				var token = _cancellation.Token;
				return _pool.Sum((dispatcher) => (dispatcher.Start(token) ? 1 : 0));
			}
		}

		public int StopAll()
		{
			lock (_pool)
			{
				return _pool.Sum((dispatcher) => (dispatcher.Stop() ? 1 : 0));
			}
		}

		public void Dispose()
		{
			Workers = 0;
		}

		public int Workers
		{
			get
			{
				return _pool.Count;
			}
			set
			{
				lock (_pool)
				{
					int count = _pool.Count;
					if (count > value)
					{
						for (int i = value; i < count; ++i)
						{
							_pool[i].Stop();
							// Don't Dispose now. Let GC does it.
						}
						_pool.RemoveRange(value, count - value);
					}
					else if (count < value)
					{
						_pool.Capacity = value;
						for (int i = count; i < value; ++i)
						{
							var new_dispatcher = _dispatcher_factory.CreateNew();
							new_dispatcher.Index = i;
							new_dispatcher.WorkingQueue = _working_queue;
							_pool.Add(new_dispatcher);
						}
					}
				}
			}
		}

		public bool TryAddEntry(T entry)
		{
			return _working_queue.TryAdd(entry);
		}

		public Task AddEntryAsync(T entry)
		{
			return Task.Run(() => _working_queue.BlockableAdd(entry, -1));
		}

		/// <summary>
		/// This method can be blocked if working queue is fulled. It will be completed when dispatcher gets a entry from queue.
		/// </summary>
		/// <param name="entry">Entry to add</param>
		/// <param name="timeout_ms">Milliseconds to wait</param>
		/// <returns></returns>
		public bool AddEntry(T entry, int timeout_ms = 0)
		{
			return _working_queue.BlockableAdd(entry, timeout_ms);
		}
	}
}
