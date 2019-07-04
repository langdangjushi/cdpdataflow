package io.infinivision.flink.connectors;

import io.infinivision.flink.connectors.cache.CacheBackend;
import io.infinivision.flink.connectors.cache.GuavaLoadingCacheBackend;
import io.infinivision.flink.connectors.cache.MapBackend;
import io.infinivision.flink.connectors.utils.CacheConfig;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheStats;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * function which can cache function result
 */
public interface CacheableFunction<K, V> {

    /**
     * cache config : size, ttl, type, refreshInterval
     *
     * @return
     */
    CacheConfig getCacheConfig();

    /**
     * get/compute a value of key
     *
     * @param key
     * @return
     */
    V loadValue(K key) throws IOException;

    /**
     * build cache
     *
     * @return
     */
    default CacheBackend<K, V> buildCache(MetricGroup metricGroup) {
        CacheConfig config = getCacheConfig();
        // cache all use concurrent map as it is more memory efficient
        if (config.isAll()) {
            return new MapBackend<>();
        }

        //  lru
        LoadingCache<K, V> loadingCache = CacheBuilder.newBuilder()
                .maximumSize(config.getSize())
                .expireAfterWrite(config.getTtl(), TimeUnit.MILLISECONDS)
                // for lru, record stats
                .recordStats()
                .build(new CacheLoader<K, V>() {

                    @Override
                    public V load(K o) throws Exception {
                        return loadValue(o);
                    }

                });
        // for lru cache, register cache metrics
        if (metricGroup != null) {
            MetricGroup group = metricGroup.addGroup("dimensionTableCache");
            group.gauge("hitRate", new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return loadingCache.stats().hitRate();
                }
            });
            group.gauge("hitCount", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return loadingCache.stats().hitCount();
                }
            });
            group.gauge("missRate", new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return loadingCache.stats().missRate();
                }
            });
            group.gauge("missCount", new Gauge<Long>() {
                @Override
                public Long getValue() {
                    return loadingCache.stats().missCount();
                }
            });
            group.gauge("loadTimens", new Gauge<Double>() {
                @Override
                public Double getValue() {
                    return loadingCache.stats().averageLoadPenalty();
                }
            });
        }
        return new GuavaLoadingCacheBackend<>(loadingCache);
    }

    default CacheBackend<K, V> buildCache() {
        return buildCache(null);
    }
}
