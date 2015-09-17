package org.opencloudb.cache.impl;

import org.apache.log4j.Logger;
import org.opencloudb.cache.CachePool;
import org.opencloudb.cache.CacheStatic;
import org.opencloudb.util.SerializeUtil;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.SafeEncoder;

public class RedisPool implements CachePool {
	private static final Logger LOGGER = Logger.getLogger(RedisPool.class);
	private String cacheName;
	private byte[] cacheNameByte;
	private int cacheSize;
	private int expireSeconds;
	private ShardedJedisPool shardedJedisPool = null;
	private final CacheStatic cacheStati = new CacheStatic();

	public RedisPool(String poolName, int cacheSize, int expireSeconds, ShardedJedisPool shardedJedisPool) {
		super();
		this.cacheName = "MyCatServerRedis." + poolName;
		this.cacheNameByte = SafeEncoder.encode(cacheName);
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

	@Override
	public void putIfAbsent(Object key, Object value) {
		ShardedJedis jedis = getJedis();
		try {
			byte[] keyBytes = SerializeUtil.serialize(key);
			if (1 == jedis.hsetnx(cacheNameByte, keyBytes, SerializeUtil.serialize(value))) { // 原先不存在,返回1,否则返回0
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
			Object ret = SerializeUtil.deserialize(jedis.hget(cacheNameByte, SerializeUtil.serialize(key)));
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
		LOGGER.info("clear cache " + cacheName);
		ShardedJedis jedis = getJedis();
		try {
			jedis.del(cacheNameByte);
			cacheStati.reset();
			cacheStati.setMemorySize(0);
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
			cacheStati.setItemSize(jedis.hlen(cacheNameByte));
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
			if (1 == jedis.hdel(cacheNameByte, SerializeUtil.serialize(key))) {
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
