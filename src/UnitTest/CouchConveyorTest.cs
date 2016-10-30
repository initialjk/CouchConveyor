
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

			var exceptions = new ConcurrentBag<Exception>();
			int semaphore = 0;
			foreach (var entry in entries)
			{
				Interlocked.Increment(ref semaphore);
				CouchConveyor.Convey(entry.Id, entry, new InstantCouchConveyorEventHandler<TestEntry>(
					(string id, string rev, TestEntry entity) =>
					{
						Interlocked.Decrement(ref semaphore);
					},
					(string id, TestEntry entity, Exception ex) =>
					{
						Interlocked.Decrement(ref semaphore);
						exceptions.Add(ex);
					}
				));
			};

			DateTime wait_started = DateTime.UtcNow;
			int remains = semaphore;
			while (semaphore > 0)
			{
				int snapshot = semaphore;
				Thread.Sleep(1000);
				Trace.WriteLine(string.Format("{0} tasks are remaining. {1} is processed", snapshot, remains - snapshot));
				remains = snapshot;

				if (exceptions.Count > 0)
				{
					throw exceptions.First();
				}
				if ((DateTime.UtcNow - wait_started).Milliseconds > 30 * 1000)
				{
					throw new TimeoutException("Too long times to process");
				}
			}

			CouchConveyor.StopAll();

			Trace.WriteLine(string.Format("Completed to store all test entries. Start checking it now"));
			var mycouch_pool = new ConcurrentBag<MyCouchStore>(Enumerable.Range(0, 100).Select((i) => new MyCouchStore(HOSTNAME, DATABASE)));

			semaphore = iteration;
			var tasks = entries.Select((entry) => Task.Run(async () =>
			{
				MyCouchStore couchdb;
				while (false == mycouch_pool.TryTake(out couchdb))
				{
					await Task.Delay(100);
				}
				var dbentry = await couchdb.GetByIdAsync<TestEntry>(entry.Id);
				mycouch_pool.Add(couchdb);
				couchdb = null;

				Assert.AreEqual(entry.Id, dbentry.Id);
				var last_entry = entries.Where((e) => e.Id == entry.Id).Last();
				if (last_entry.IntValue != dbentry.IntValue)
				{
					var es = entries.Where((e) => e.Id == entry.Id);
					Trace.WriteLine(string.Format("Entry '{0}' has stored incorrectly: {1}", entry.Id, es.ToString()));
				}
				Assert.AreEqual(last_entry.IntValue, dbentry.IntValue);
				Assert.AreEqual(last_entry.IntNullValue, dbentry.IntNullValue);
				Assert.AreEqual(last_entry.StringValue, dbentry.StringValue);
				Assert.AreEqual(last_entry.StringNullValue, dbentry.StringNullValue);
				Assert.AreEqual(last_entry.CustomValue, dbentry.CustomValue);
				Assert.AreEqual(last_entry.CustomNullValue, dbentry.CustomNullValue);
				Assert.AreEqual(last_entry.DateTimeValue.ToString(), dbentry.DateTimeValue.ToString());
				CollectionAssert.AreEqual(last_entry.ListValue, dbentry.ListValue);
			}));

			Task.WaitAll(tasks.ToArray());
			foreach (var c in mycouch_pool)
			{
				c.Dispose();
			}

		}
	}
}
