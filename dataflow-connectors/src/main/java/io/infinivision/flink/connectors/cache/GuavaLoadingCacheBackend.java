package io.infinivision.flink.connectors.cache;

import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;

public class GuavaLoadingCacheBackend<K, V> implements CacheBackend<K, V> {

    private LoadingCache<K, V> cache;

    public GuavaLoadingCacheBackend(LoadingCache<K, V> cache) {
        this.cache = cache;
    }

    @Override
    public V get(K k) {
        try {
            return this.cache.get(k);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(K k, V v) {
        this.cache.put(k, v);
    }

    @Override
    public boolean exist(K k) {
        return this.cache.getIfPresent(k) != null;
    }
}

