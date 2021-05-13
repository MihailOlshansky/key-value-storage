package com.itmo.java.basics.logic.impl;

import java.util.LinkedHashMap;

import com.itmo.java.basics.logic.DatabaseCache;

public class DatabaseCacheImpl implements DatabaseCache {

    public static int DB_CACHE_CAPACITY = 5_000;

    private class CacheMap extends LinkedHashMap<String, byte[]> {
        public CacheMap() {
            super(DB_CACHE_CAPACITY, 1f, true);
        }

        @Override
        protected boolean removeEldestEntry(java.util.Map.Entry<String, byte[]> eldest) {
            return size() > DB_CACHE_CAPACITY;
        }
    }

    private final CacheMap cacheMap = new CacheMap();

    @Override
    public byte[] get(String key) {
        return cacheMap.get(key);
    }

    @Override
    public void set(String key, byte[] value) {
        cacheMap.put(key, value);
    }

    @Override
    public void delete(String key) {
        cacheMap.remove(key);    
    }
}
