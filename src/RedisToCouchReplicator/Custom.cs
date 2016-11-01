using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CouchConveyor.Redis;

namespace RedisToCouchReplicator
{
	class PipeTokenedNotificationMessageHandler : InformedChangeNotificationHandler
	{
		static log4net.ILog Logger = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

		public override void OnNotified(string channel, string message)
		{
			var args = message.Split(new char[] { '|' }, 5);
			if (args.Length < 3)
			{
				Logger.WarnFormat("Message has invalid format [{0}]: {1}", channel, message);
				return;
			}

			Logger.DebugFormat("Message recevied [{0}]: {1}", channel, message);
			var command = args[1];
			var redis_key = args[2];
			if (string.Equals("SetDictionary", args[1])) 
			{
				Replicator.CopyHashValue(this.ReplicationConfig.RedisSourceKey, redis_key, this.ReplicationConfig.CouchTargetDatabase);
			}
			else if (string.Equals("RemoveDictionary", args[1]))
			{
				Replicator.DeleteCouchDocument(this.ReplicationConfig.CouchTargetDatabase, redis_key);
			}
			else
			{
				Logger.WarnFormat("Message has unrecognized argument '{0}' [{1}]: {2}", args[1], channel, message);
			}
		}
	}
}
