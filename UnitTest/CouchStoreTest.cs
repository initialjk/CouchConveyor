
using System;
using System.Text;
using System.Collections.Generic;
using CouchStore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using MyCouch;
using System.Linq;

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
			if (false == obj is TestInner) {
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

	public class TestWaitEventHandler : CouchStoreWaitEventHandler<TestEntry>
	{
		public TestWaitEventHandler(EventWaitHandle handle) : base(handle) { }
	}

	/// <summary>
	/// Test basic functions of CouchStore Module. To execute this teest, an CouchDb is required on localhost:5984
	/// </summary>
	[TestClass]
	public class CouchStoreBasicFunctionTest
	{
		[AssemblyInitialize]
		public static void Configure(TestContext tc)
		{
			log4net.Config.XmlConfigurator.Configure();
		}

		public PooledCouchStore<TestEntry> CouchStore { get; set; }
		public static string HOSTNAME = "http://localhost:5984";
		public static string DATABASE = "test_db";

		[ClassInitialize()]
		public static void SetupTest(TestContext testContext)
		{
			//testContext.Properties["CouchStore"] = new PooledCouchStore<TestEntry>(new CouchStoreDispatcherFactory<TestEntry>(HOSTNAME, "test_db"), 10);
		}

		public CouchStoreBasicFunctionTest()
		{
			this.CouchStore = new PooledCouchStore<TestEntry>(HOSTNAME, DATABASE, 10);
			this.CouchStore.StartAll();
		}

		private TestContext testContextInstance;
		public TestContext TestContext
		{
			get
			{
				return testContextInstance;
			}
			set
			{
				testContextInstance = value;
			}
		}

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

		public List<T> MakeList<T>(Func<int, T> creator, int from, int to)
		{
			return new List<T>(Enumerable.Range(from, to).Select(i => creator.Invoke(i)));
		}

		public TestEntry CreateTestEntry(string prefix, int index)
		{
			var rand = new Random();
			var base_id = string.Format("{0}-{1}", prefix, index);
			var temp_dict = new Dictionary<string, string>();
			temp_dict[prefix] = base_id;
			return new TestEntry
			{
				Id = base_id,
				IntValue = rand.Next(),
				StringValue = "Test String for " + base_id,
				DateTimeValue = DateTime.UtcNow,
				CustomValue = new TestInner
				{
					Id = base_id + ":CustomValue",
					IntValue = rand.Next(),
					StringValue = "Test nested value's string for " + base_id,
					IntArrayValue = MakeList((i) => { return rand.Next(); }, 0, rand.Next(10)).ToArray(),
				},
				ListValue = MakeList( (i) => {
					return new TestInner
					{
						Id = base_id + ":CustomList:" + i,
						IntValue = rand.Next(),
						StringValue = "Test nested lists's string for " + base_id + " - " + i,
						IntArrayValue = MakeList((x) => { return rand.Next(); }, 0, rand.Next(10)).ToArray(),
					};
				}, 0, rand.Next(0) ),
				DictionaryValue = temp_dict,
			};
		}

		[TestMethod]
		public void TestStoreNestedEntry()
		{
			ManualResetEvent wait_event = new ManualResetEvent(false);
			this.CouchStore.Store("TestPushEntry", CreateTestEntry("a-test-entry", 1), new TestWaitEventHandler(wait_event));
			wait_event.WaitOne(30 * 1000);
		}

		private static int CompareTestEntryByIntValue(TestEntry x, TestEntry y)
		{
			return x.IntValue.CompareTo(y.IntValue);
		}

		[TestMethod]
		public void TestMassiveStoreEntries()
		{
			var entries = new List<TestEntry>(3000);
			for (int i = 0; i < 1000; ++i)
			{
				for (int n = 0; n < 5; ++n)
				{
					entries.Add(CreateTestEntry("massive-test-entry", i));
				}
			}
			entries.Sort(CompareTestEntryByIntValue); // shuffle randomly
			
			var wait_events = new ConcurrentBag<ManualResetEvent>();
			Parallel.ForEach(entries, (entry) =>
			{
				var wait_event = new ManualResetEvent(false);
				wait_events.Add(wait_event);
				this.CouchStore.Store("TestPushEntry", entry, new TestWaitEventHandler(wait_event));
			});

			WaitHandle.WaitAll(wait_events.ToArray(), 30 * 1000);


			Task.Run(async () =>
			{
				var couchdb = new MyCouchStore(HOSTNAME, DATABASE);
				foreach (var entry in entries)
				{
					var dbentry = await couchdb.GetByIdAsync<TestEntry>(entry.Id);
					Assert.AreEqual(entry.Id, dbentry.Id);
					var last_entry = entries.Where((e) => e.Id == entry.Id).OrderBy((e) => e.IntValue).Last();
					Assert.AreEqual(entry.IntValue, dbentry.IntValue);
					Assert.AreEqual(entry.IntNullValue, dbentry.IntNullValue);
					Assert.AreEqual(entry.StringValue, dbentry.StringValue);
					Assert.AreEqual(entry.StringNullValue, dbentry.StringNullValue);
					Assert.AreEqual(entry.CustomValue, dbentry.CustomValue);
					Assert.AreEqual(entry.CustomNullValue, dbentry.CustomNullValue);
					Assert.AreEqual(entry.ListValue, dbentry.ListValue);
					Assert.AreEqual(entry.DateTimeValue, dbentry.DateTimeValue);
				}
			}).Wait();
		}
	}
}
