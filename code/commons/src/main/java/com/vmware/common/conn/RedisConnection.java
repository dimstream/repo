package com.vmware.common.conn;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;

import com.vmware.common.dim.config.AppConfig;

/**
 * Class to establish connection with the Redis cluster.
 * 
 * @author vedanthr
 */
public class RedisConnection {

	static JedisCommands jedis = null;

	private static final Logger logger = LogManager.getLogger(RedisConnection.class);

	/**
	 * Initializes the connection to Redis cluster.
	 * 
	 * @param ac Application configuration
	 */
	public static void init(AppConfig ac) {
		logger.traceEntry();
		if (jedis == null) {
			Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
			for (int i = 0; i < ac.cacheEngine.host.size(); i++) {
				jedisClusterNode.add(new HostAndPort(ac.cacheEngine.host.get(i), ac.cacheEngine.port.get(i)));
			}

			if (ac.cacheEngine.host.size() == 1) {
				jedis = new Jedis(ac.cacheEngine.host.get(0), ac.cacheEngine.port.get(0));
			} else {
				jedis = new JedisCluster(jedisClusterNode);
			}
		}
		logger.traceExit();
	}

	/**
	 * Initializes the connection to Redis cluster.
	 * 
	 * @param redisMachine Redis cluster hosts
	 * @param redisPort Redis cluster ports
	 * 
	 * @throws SQLException SQLException
	 */
	public static void init(List<String> redisMachine, List<Integer> redisPort) throws SQLException {
		if (jedis == null) {
			Set<HostAndPort> jedisClusterNode = new HashSet<HostAndPort>();
			for (int i = 0; i < redisMachine.size(); i++) {
				jedisClusterNode.add(new HostAndPort(redisMachine.get(i), redisPort.get(i)));
			}
			if (jedisClusterNode.size() > 1) {
				jedis = new JedisCluster(jedisClusterNode);
			} else {
				jedis = new Jedis(redisMachine.get(0), redisPort.get(0));
			}
		}
	}

	/**
	 * Retrieve Redis cluster connection.
	 * 
	 * @return Redis Connection Object
	 * @throws SQLException SQLException
	 */
	public static JedisCommands getConnection() throws SQLException {

		logger.traceEntry();
		if(jedis == null){
			logger.debug("System.getenv(REDIS_CONN_STR) - " + System.getenv("REDIS_CONN_STR"));
			String redisConf = System.getenv("REDIS_CONN_STR");
			if(redisConf != null){
				String[] hostPortList = redisConf.split(",");
				List<String> hosts = new ArrayList<String>();
				List<Integer> ports = new ArrayList<Integer>();
				for(String hostPort : hostPortList){
					String[] hp = hostPort.split(":");
					hosts.add(hp[0]);
					ports.add(Integer.valueOf(hp[1]));
				}
				init(hosts, ports);				
			}
		}
		return logger.traceExit(jedis);
	}
}