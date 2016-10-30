using CouchConveyor.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RedisToCouchReplicator
{
	class Program
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		enum CtrlType : uint
		{
			CTRL_C_EVENT = 0,
			CTRL_BREAK_EVENT = 1,
			CTRL_CLOSE_EVENT = 2,
			CTRL_LOGOFF_EVENT = 5,
			CTRL_SHUTDOWN_EVENT = 6
		}

		private delegate bool ConsoleEventHandler(CtrlType sig);
		[DllImport("Kernel32")]
		private static extern bool SetConsoleCtrlHandler(ConsoleEventHandler handler, bool add);

		static int Main(string[] args)
		{
			// TODO: Extract this to configuration
			var result = log4net.Config.BasicConfigurator.Configure();

			string couchdb_host = "http://localhost:5984";
			string couchdb_name = "config";
			string config_id = null;
			switch (args.Length)
			{
				case 3:
					config_id = args[2];
					goto case 2;
				case 2:
					couchdb_name = args[1];
					goto case 1;
				case 1:
					couchdb_host = args[0];
					break;
			}


			var cts = new Replicator().Start(couchdb_host, couchdb_name, config_id);
			Logger.Info("Replicator has started.");
			
			SetConsoleCtrlHandler((CtrlType) => { cts.Cancel(); return true; }, true);

			cts.Token.WaitHandle.WaitOne();
			System.Console.ReadKey();
			return 0;
		}
	}
}