
using System;
using System.Text;
using System.Collections.Generic;
using CouchConveyor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using MyCouch;
using System.Linq;
using System.Diagnostics;

namespace UnitTest
{
	public class TestInner
	{
		public string Id { get; set; }
		public int IntValue { get; set; }
		public string StringValue { get; set; }
		public int[] IntArrayValue { get; set; }

		public override bool Equals(object obj)
		{
			if (false == obj is TestInner)
			{
				return false;
			}
			var rhs = obj as TestInner;
			if ((System.Object)rhs == null)
			{
				return false;
			}
			return this.Id == rhs.Id
				&& this.IntValue == rhs.IntValue
				&& string.Equals(this.StringValue, rhs.StringValue)
				&& Enumerable.SequenceEqual(this.IntArrayValue, rhs.IntArrayValue);
		}

		public override int GetHashCode()
		{
			return (Id ?? "").GetHashCode();
		}
	}

	public class TestEntry
	{
		public string Id { get; set; }
		public string StringValue { get; set; }
		public string StringNullValue { get; set; }
		public int IntValue { get; set; }
		public int IntNullValue { get; set; }
		public DateTime DateTimeValue { get; set; }
		public List<TestInner> ListValue { get; set; }
		public Dictionary<string, string> DictionaryValue { get; set; }
		public TestInner CustomValue { get; set; }
		public TestInner CustomNullValue { get; set; }
	}

	public class TestWaitEventHandler : CouchConveyorWaitEventHandler<TestEntry>
	{
		public TestWaitEventHandler(EventWaitHandle handle) : base(handle) { }
	}

	/// <summary>
	/// Test basic functions of CouchConveyor Module. To execute this teest, an CouchDb is required on localhost:5984
	/// </summary>
	[TestClass]
	public class CouchConveyorBasicFunctionTest
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		[AssemblyInitialize]
		public static void Configure(TestContext tc)
		{
			var debug_appender = new log4net.Appender.DebugAppender();
			debug_appender.ImmediateFlush = true;
			debug_appender.ActivateOptions();
			var result = log4net.Config.BasicConfigurator.Configure(debug_appender);
		}

		public static string HOSTNAME = "http://localhost:5984";
		public static string DATABASE = "test_db";

		[ClassInitialize()]
		public static void SetupTest(TestContext testContext)
		{
		}

		public TestContext TestContext { get; set; }

		#region Additional test attributes
		//
		// You can use the following additional attributes as you write your tests:
		//
		// Use ClassInitialize to run code before running the first test in the class
		// [ClassInitialize()]
		// public static void MyClassInitialize(TestContext testContext) { }
		//
		// Use ClassCleanup to run code after all tests in a class have run
		// [ClassCleanup()]
		// public static void MyClassCleanup() { }
		//
		// Use TestInitialize to run code before running each test 
		// [TestInitialize()]
		// public void MyTestInitialize() { }
		//
		// Use TestCleanup to run code after each test has run
		// [TestCleanup()]
		// public void MyTestCleanup() { }
		//
		#endregion

		public List<T> MakeList<T>(int from, int to, Func<int, T> creator)
		{
			return new List<T>(Enumerable.Range(from, to).Select(i => creator.Invoke(i)));
		}

		public static readonly Random Rand = new Random();

		public TestEntry CreateTestEntry(string prefix, int index)
		{
			var base_id = string.Format("{0}-{1}", prefix, index);
			var temp_dict = new Dictionary<string, string>();
			temp_dict[prefix] = base_id;
			return new TestEntry
			{
				Id = base_id,
				IntValue = Rand.Next(),
				StringValue = "Test String for " + base_id,
				DateTimeValue = DateTime.UtcNow,
				CustomValue = new TestInner
				{
					Id = base_id + ":CustomValue",
					IntValue = Rand.Next(),
					StringValue = "Test nested value's string for " + base_id,
					IntArrayValue = MakeList(0, Rand.Next(10), (i) => { return Rand.Next(); }).ToArray(),
				},
				ListValue = MakeList(0, Rand.Next(5), (i) =>
				{
					return new TestInner
					{
						Id = base_id + ":CustomList:" + i,
						IntValue = Rand.Next(),
						StringValue = "Test nested lists's string for " + base_id + " - " + i,
						IntArrayValue = MakeList(0, Rand.Next(10), (x) => { return Rand.Next(); }).ToArray(),
					};
				}),
				DictionaryValue = temp_dict,
			};
		}

		[TestMethod]
		public void TestStoreNestedEntry()
		{
			var CouchConveyor = new OrderedPooledCouchConveyor<TestEntry>(HOSTNAME, DATABASE, 2);
			CouchConveyor.StartAll();

			ManualResetEvent wait_event = new ManualResetEvent(false);
			CouchConveyor.Convey("TestStoreNestedEntry", CreateTestEntry("a-test-entry", 1), new TestWaitEventHandler(wait_event));
			wait_event.WaitOne(30 * 1000);

			CouchConveyor.StopAll();
		}

		public class TestMonitor<T> where T : class
		{
			private /*volatile*/ int _semaphore;
			public int Remains { get { return _semaphore; } }
			public DateTime Started { get; set; }
			public ConcurrentBag<Exception> Exceptions { get; private set; }
			public float TimeoutSeconds { get; set; }

			public TestMonitor(float timeout_seconds = 30)
			{
				_semaphore = 0;
				Started = DateTime.UtcNow;
				Exceptions = new ConcurrentBag<Exception>();
				TimeoutSeconds = timeout_seconds;
			}

			public InstantCouchConveyorEventHandler<T> StartOne()
			{
				Interlocked.Increment(ref this._semaphore);
				return new InstantCouchConveyorEventHandler<T>(this.OnSuccessOne, this.OnFailedOne);
			}

			public virtual void OnSuccessOne(string id, string rev, T entity)
			{
				FinishOne();
			}

			public virtual void OnFailedOne(string id, T entity, Exception ex)
			{
				FinishOne();
				this.Exceptions.Add(ex);
			}

			protected void FinishOne()
			{
				Interlocked.Decrement(ref this._semaphore);
			}

			public int WaitToFinish()
			{
				var wait_started = DateTime.UtcNow;
				var remains = this.Remains;

				while (_semaphore > 0)
				{
					int snapshot = _semaphore;
					Thread.Sleep(1000);
					var time_from_start = DateTime.UtcNow - Started;
					Trace.WriteLine(string.Format("[{2}.{3}] {0} tasks are remaining. {1} is processed", snapshot, remains - snapshot, time_from_start.Seconds, time_from_start.Milliseconds));
					remains = snapshot;

					if (Exceptions.Count > 0)
					{
						throw Exceptions.First();
					}
					if ((DateTime.UtcNow - wait_started).TotalMilliseconds > (int)this.TimeoutSeconds * 1000.0f)
					{
						throw new TimeoutException("Too long times to process, still remains: " + _semaphore);
					}
				}
				return _semaphore;
			}
		}

		public class TestMonitorForDelete<T> : TestMonitor<T> where T : class
		{
			public override void OnFailedOne(string id, T entity, Exception ex)
			{
				FinishOne();

				var mcrex = ex as MyCouchResponseException;
				if (mcrex != null || mcrex.HttpStatus == System.Net.HttpStatusCode.NotFound || "DELETE".Equals(mcrex.HttpMethod))
				{
					// Skip this
				}
				else 
				{
					this.Exceptions.Add(ex);
				}
			}
		}

		[TestMethod]
		public void TestMassiveStoreEntries()
		{
			var iteration = 1000;
			var entries = new List<TestEntry>(3000);
			for (int i = 0; i < iteration; ++i)
			{
				for (int n = 0; n < 10; ++n)
				{
					var entry = CreateTestEntry("massive-test-entry", i);
					entry.StringValue += " :" + n;
					entries.Add(entry);
				}
			}
			entries.Sort(delegate(TestEntry x, TestEntry y) { return x.IntValue.CompareTo(y.IntValue); }); // shuffle randomly
			
			var CouchConveyor = new OrderedPooledCouchConveyor<TestEntry>(HOSTNAME, DATABASE, 1024);
			CouchConveyor.StartAll();

			var monitor = new TestMonitor<TestEntry>();

			foreach (var entry in entries)
			{
				CouchConveyor.Convey(entry.Id, entry, monitor.StartOne());
			};

			monitor.WaitToFinish();

			Trace.WriteLine(string.Format("Completed to store all test entries. Start checking it now"));
			var mycouch_pool = new ConcurrentBag<MyCouchStore>(Enumerable.Range(0, 100).Select((i) => new MyCouchStore(HOSTNAME, DATABASE)));
			var entries_to_remove = new ConcurrentBag<TestEntry>();

			// Async tasks are suppressing Assert exceptions
			Parallel.ForEach(entries, new ParallelOptions { MaxDegreeOfParallelism = 100 }, (entry) =>
			{
				MyCouchStore couchdb;
				if (false == mycouch_pool.TryTake(out couchdb)) // Actually, this condition is not required. Just to avoid CS0165
				{
					SpinWait.SpinUntil(() => mycouch_pool.TryTake(out couchdb));
				}
				var task = couchdb.GetByIdAsync<TestEntry>(entry.Id);
				var dbentry = task.Result;
				mycouch_pool.Add(couchdb);
				couchdb = null;

				Assert.IsNotNull(dbentry);

				var last_entry = entries.Where((e) => e.Id == entry.Id).Last();
				if (last_entry.IntValue != dbentry.IntValue)
				{
					var es = entries.Where((e) => e.Id == entry.Id);
					Trace.WriteLine(string.Format("Entry '{0}' has stored incorrectly: {1} for {2}", entry.Id, dbentry.IntValue, string.Join(",", es.Select((e)=>e.IntValue))));
				}
				Assert.AreEqual(entry.Id, dbentry.Id);
				Assert.AreEqual(last_entry.IntValue, dbentry.IntValue);
				Assert.AreEqual(last_entry.IntNullValue, dbentry.IntNullValue);
				Assert.AreEqual(last_entry.StringValue, dbentry.StringValue);
				Assert.AreEqual(last_entry.StringNullValue, dbentry.StringNullValue);
				Assert.AreEqual(last_entry.CustomValue, dbentry.CustomValue);
				Assert.AreEqual(last_entry.CustomNullValue, dbentry.CustomNullValue);
				Assert.AreEqual(last_entry.DateTimeValue.ToString(), dbentry.DateTimeValue.ToString());
				CollectionAssert.AreEqual(last_entry.ListValue, dbentry.ListValue);

				if (last_entry.IntValue == entry.IntValue)
				{
					entries_to_remove.Add(entry);
				}
			});
						
			var delete_monitor = new TestMonitorForDelete<TestEntry>();
			Parallel.ForEach(entries_to_remove, (entry) => CouchConveyor.Convey(entry.Id, null, delete_monitor.StartOne()));						
			delete_monitor.WaitToFinish();
			CouchConveyor.StopAll();

			Parallel.ForEach(entries, new ParallelOptions { MaxDegreeOfParallelism = 100 }, (entry) =>
			{
				MyCouchStore couchdb;
				if (false == mycouch_pool.TryTake(out couchdb))
				{
					SpinWait.SpinUntil(() => mycouch_pool.TryTake(out couchdb));
				}
				var task = couchdb.GetByIdAsync<TestEntry>(entry.Id);
				var dbentry = task.Result; 
				mycouch_pool.Add(couchdb);
				couchdb = null;

				Assert.IsNull(dbentry);
			});

			foreach (var c in mycouch_pool)
			{
				c.Dispose();
			}

		}
	}
}
