package org.opencloudb.cache.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CachePoolFactory;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedisPool;

import com.google.common.collect.Lists;

public class RedisPoolFactory extends CachePoolFactory {
	private static final Logger LOGGER = Logger.getLogger(RedisPoolFactory.class);
	private static ShardedJedisPool shardedJedisPool = null;

	static {
		try {
			init();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private static void init() throws Exception {
		Properties props = new Properties();
		props.load(RedisPoolFactory.class.getResourceAsStream("/redis_conf.properties"));
		String redisClusterNodes = props.getProperty("redis.cluster.nodes");

		List<JedisShardInfo> shards = Lists.newArrayList();
		for (String clusterNode : redisClusterNodes.split(",")) {
			String[] nodeIpPort = clusterNode.split(":");
			LOGGER.info("创建Redis连接:" + Arrays.toString(nodeIpPort));
			JedisShardInfo shard = new JedisShardInfo(nodeIpPort[0], Integer.parseInt(nodeIpPort[1]));
			shards.add(shard);
		}

		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();

		poolConfig.setMaxTotal(Integer.parseInt(props.getProperty("redis.pool.MaxTotal", "100")));
		poolConfig.setMaxIdle(Integer.parseInt(props.getProperty("redis.pool.MaxIdle", "20")));
		poolConfig.setMaxWaitMillis(Long.parseLong(props.getProperty("redis.pool.MaxWaitMillis", "1000")));
		poolConfig.setTestOnBorrow(Boolean.parseBoolean(props.getProperty("redis.pool.TestOnBorrow", "true")));

		shardedJedisPool = new ShardedJedisPool(poolConfig, shards);
		// ShardedJedis jedis = pool.getResource();
		// jedis.close();
	}

	public static void destory() {
		shardedJedisPool.close();
	}

	@Override
	public CachePool createCachePool(String poolName, int cacheSize, int expireSeconds) {
		RedisPool redisPool = new RedisPool(poolName, cacheSize, expireSeconds, shardedJedisPool);
		LOGGER.info("创建Redis缓存池实例:" + poolName);
		return redisPool;
	}

}
