package io.infinivision.flink.connectors.cache;

/**
 * cache back end
 * @param <K>
 * @param <V>
 */
public interface CacheBackend<K, V> {
    V get(K k);

    void put(K k, V v);

    boolean exist(K k);
}
