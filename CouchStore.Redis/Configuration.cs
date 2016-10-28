using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CouchStore;

namespace CouchStore.Redis
{
	public class ReplicatorConfiguration : IConfiguration<ReplicatorConfiguration>
	{
		public class Replication
		{
			public string RedisSourceKey { get; set; }
			public string CouchTargetDatabase { get; set; }
		}

		public string CouchDbEndpoint { get; set; }
		public string[] RedisServers { get; set; }
		public string[] ChannelsToSubscribe { get; set; }
		public Replication[] HashReplications { get; set; }
		public float HashReplicationIntervalSeconds { get; set; }

		public int WriteScaleDefault { get; set; }
		public Dictionary<string, int> WriteScalePerDatabase { get; set; }
		public int RedisConnectionPoolSize { get; set; }
		public int RedisPoolTimeoutSeconds { get; set; }

		public ReplicatorConfiguration()
		{
			Id = this.GetType().FullName;

			CouchDbEndpoint = "http://localhost:5984";
			RedisServers = new string[] { "localhost" };
			HashReplications = new Replication[] { };
			ChannelsToSubscribe = new string[] { };
			HashReplicationIntervalSeconds = 0.0f;

			WriteScaleDefault = Environment.ProcessorCount * 4;
			WriteScalePerDatabase = new Dictionary<string, int>();
			RedisConnectionPoolSize = 30;
			RedisPoolTimeoutSeconds = 20;
		}

		public override bool PreLoad()
		{
			if (string.IsNullOrWhiteSpace(Id))
			{
				Id = this.GetType().FullName;
			}
			return string.IsNullOrWhiteSpace(CouchDbEndpoint) == false
				&& RedisServers != null && RedisServers.All((e) => !string.IsNullOrWhiteSpace(e))
				&& WriteScaleDefault > 0
				&& RedisConnectionPoolSize > 0;
		}

		public override bool PreChange(ReplicatorConfiguration value, string[] properties)
		{
			return true;
		}
	}
}
