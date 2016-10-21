
using System;
using System.Text;
using System.Collections.Generic;
using CouchStore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading;

namespace UnitTest
{
	public class TestInner
	{
		public string Id { get; set; }
		public string StringValue { get; set; }
		public int IntValue { get; set; }
		public int[] IntArrayValue { get; set; }
	}

	public class TestEntry
	{
		public string Id { get; set; }
		public string StringValue { get; set; }
		public int IntValue { get; set; }
		public TestInner CustomValue { get; set; }
		public List<TestInner> CustomListValue { get; set; }
		public DateTime TimeStamp { get; set; }
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
		public PooledCouchStore<TestEntry> CouchStore { get; set; }
		public static string HOSTNAME = "http://localhost:5984";

		[ClassInitialize()]
		public static void SetupTest(TestContext testContext)
		{
			//testContext.Properties["CouchStore"] = new PooledCouchStore<TestEntry>(new CouchStoreDispatcherFactory<TestEntry>(HOSTNAME, "test_db"), 10);
		}

		public CouchStoreBasicFunctionTest()
		{
			this.CouchStore = new PooledCouchStore<TestEntry>(HOSTNAME, "test_db", 10);
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

		public TestEntry CreateTestEntry()
		{
			return new TestEntry
			{
				Id = "TestPushEntry",
				IntValue = 0,
				StringValue = "Test String",
				TimeStamp = DateTime.UtcNow,
				CustomValue = new TestInner
				{
					Id = "TestPushEntry:1",
					IntValue = 1,
					StringValue = "Test String:1",
					IntArrayValue = new int[] { 1, 1 }
				},
				CustomListValue = new List<TestInner>(new TestInner[]
				{
					new TestInner
					{
						Id = "TestPushEntry:2-1",
						IntValue = 21,
						StringValue = "Test String:2-1",
						IntArrayValue = new int[] { 2,1,1,1 }
					},
					new TestInner
					{
						Id = "TestPushEntry:2-2",
						IntValue = 22,
						StringValue = "Test String:2-2",
						IntArrayValue = new int[] { 2,1,2,1 }
					}
				}),
			};
		}

		[TestMethod]
		public void TestPushEntry()
		{
			ManualResetEvent wait_event = new ManualResetEvent(false);
			this.CouchStore.Store("TestPushEntry", CreateTestEntry(), new TestWaitEventHandler(wait_event));
			wait_event.WaitOne(30 * 1000);
		}
	}
}
