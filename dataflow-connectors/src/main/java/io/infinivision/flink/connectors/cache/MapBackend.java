package io.infinivision.flink.connectors.cache;

import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import java.util.concurrent.ConcurrentMap;

public class MapBackend<K, V> implements CacheBackend<K, V> {

    private ConcurrentMap<K, V> cache;

    public MapBackend() {
        cache = Maps.newConcurrentMap();
    }

    @Override
    public V get(K k) {
        return this.cache.get(k);
    }

    @Override
    public void put(K k, V v) {
        this.cache.put(k, v);
    }

    @Override
    public boolean exist(K k) {
        return this.cache.containsKey(k);
    }

}
