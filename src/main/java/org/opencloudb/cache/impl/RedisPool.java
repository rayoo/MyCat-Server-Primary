package org.opencloudb.cache.impl;

import java.util.Set;

import org.apache.log4j.Logger;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CacheStatic;
import org.opencloudb.util.ByteUtil;
import org.opencloudb.util.CollectionUtil;
import org.opencloudb.util.SerializeUtil;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.SafeEncoder;

public class RedisPool implements CachePool {
	private static final Logger LOGGER = Logger.getLogger(RedisPool.class);
	// private static final String CMD_OK = "OK";
	private String cacheName;
	private byte[] cacheNameByte;
	private byte[] cacheNameByteMatch;
	private int cacheSize;
	private int expireSeconds;
	private ShardedJedisPool shardedJedisPool = null;
	private final CacheStatic cacheStati = new CacheStatic();

	public RedisPool(String poolName, int cacheSize, int expireSeconds, ShardedJedisPool shardedJedisPool) {
		super();
		this.cacheName = "MyCatServer." + poolName;
		this.cacheNameByte = SafeEncoder.encode(cacheName);
		this.cacheNameByteMatch = SafeEncoder.encode(cacheName + "*");
		this.cacheSize = cacheSize;
		this.expireSeconds = expireSeconds;
		this.shardedJedisPool = shardedJedisPool;
		cacheStati.setMaxSize(getMaxSize());
	}

	private ShardedJedis getJedis() {
		ShardedJedis jedis = shardedJedisPool.getResource();
		return jedis;
	}

	private void closeJedis(ShardedJedis jedis) {
		if (null != jedis) {
			jedis.close();
		}
	}

	private byte[] buildKeyBytes(Object secondKey) {
		return ByteUtil.byteMerge(cacheNameByte, SerializeUtil.serialize(secondKey)); // 合并两个数组;
	}

	@Override
	public void putIfAbsent(Object key, Object value) {
		ShardedJedis jedis = getJedis();
		try {
			byte[] keyBytes = buildKeyBytes(key);
			if (1 == jedis.setnx(keyBytes, SerializeUtil.serialize(value))) { // 原先不存在,返回1,否则返回0
				jedis.expire(keyBytes, expireSeconds);
				cacheStati.incPutTimes();
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(new StringBuilder(cacheName).append(" add cache ,key:").append(key).append(" value:").append(value));
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
			throw new RuntimeException(e);
		} finally {
			closeJedis(jedis);
		}
	}

	@Override
	public Object get(Object key) {
		ShardedJedis jedis = getJedis();
		try {
			byte[] keyBytes = buildKeyBytes(key);
			Object ret = SerializeUtil.deserialize(jedis.get(keyBytes));
			if (null != ret) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(cacheName + " hit cache ,key:" + key);
				}
				cacheStati.incHitTimes();
				return ret;
			} else {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(cacheName + "  miss cache ,key:" + key);
				}
				cacheStati.incAccessTimes();
				return null;
			}
		} catch (Exception e) {
			LOGGER.error(e);
			throw new RuntimeException(e);
		} finally {
			closeJedis(jedis);
		}
	}

	@Override
	public void clearCache() {
		ShardedJedis jedis = getJedis();
		try {
			long delCount = 0;
			for (Jedis shardJedis : jedis.getAllShards()) {
				Set<byte[]> shardKeys = shardJedis.keys(cacheNameByteMatch);
				if (!CollectionUtil.isEmpty(shardKeys)) {
					delCount += shardJedis.del(shardKeys.toArray(new byte[0][0]));
				}
				shardJedis.close();
			}
			cacheStati.reset();
			cacheStati.setMemorySize(0);
			LOGGER.info("clear cache " + cacheName + ", clear size:" + delCount);
		} catch (Exception e) {
			LOGGER.error(e);
			throw new RuntimeException(e);
		} finally {
			closeJedis(jedis);
		}
	}

	@Override
	public CacheStatic getCacheStatic() {
		ShardedJedis jedis = getJedis();
		try {
			long itemSize = 0;
			for (Jedis shardJedis : jedis.getAllShards()) {
				Set<byte[]> shardKeys = shardJedis.keys(cacheNameByteMatch);
				if (!CollectionUtil.isEmpty(shardKeys)) {
					itemSize += shardKeys.size();
				}
				shardJedis.close();
			}
			cacheStati.setItemSize(itemSize);
		} catch (Exception e) {
			LOGGER.error(e);
			throw new RuntimeException(e);
		} finally {
			closeJedis(jedis);
		}
		return cacheStati;
	}

	@Override
	public long getMaxSize() {
		return cacheSize;
	}

	@Override
	public void remove(Object key) {
		ShardedJedis jedis = getJedis();
		try {
			byte[] keyBytes = buildKeyBytes(key);
			if (1 == jedis.del(keyBytes)) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug(cacheName + " remove cache ,key:" + key);
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
			throw new RuntimeException(e);
		} finally {
			closeJedis(jedis);
		}
	}

}
