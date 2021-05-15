package com.itmo.java.basics.logic.impl;

import java.util.LinkedHashMap;

import com.itmo.java.basics.logic.DatabaseCache;

public class DatabaseCacheImpl implements DatabaseCache {
<<<<<<< HEAD
    private static final int CAPACITY = 5_000;
=======

    public static int CAPACITY = 5_000;

    private class CacheMap extends LinkedHashMap<String, byte[]> {
        public CacheMap() {
            super(CAPACITY, 1f, true);
        }

        @Override
        protected boolean removeEldestEntry(java.util.Map.Entry<String, byte[]> eldest) {
            return size() > CAPACITY;
        }
    }

    private final CacheMap cacheMap = new CacheMap();
>>>>>>> 2c4f880 (Lab2 (#2))

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
