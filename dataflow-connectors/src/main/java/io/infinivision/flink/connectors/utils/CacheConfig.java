package io.infinivision.flink.connectors.utils;

import io.infinivision.flink.connectors.utils.CommonTableOptions.CacheType;
import org.apache.flink.table.util.TableProperties;

import java.io.Serializable;

import static io.infinivision.flink.connectors.utils.CommonTableOptions.*;

public class CacheConfig implements Serializable {

    private static final long serialVersionUID = 3178091770400531150L;

    private CacheType type;
    private long size;
    private long ttl;

    public CacheConfig(String type, int size, int ttl) {
        this.type = CacheType.valueOf(type);
        this.size = size;
        this.ttl = ttl;
    }

    public CacheConfig(String type, String size, String ttl) {
        this(type, Integer.valueOf(size), Integer.valueOf(ttl));
    }

    public static CacheConfig fromTableProperty(TableProperties tableProperties) {
        return new CacheConfig(tableProperties.getString(CACHE),
                tableProperties.getString(CACHE_SIZE),
                tableProperties.getString(CACHE_TTL));
    }

    public boolean hasCache() {
        return !(type == CacheType.NONE);
    }

    public boolean isLRU() {
        return type == CacheType.LRU;
    }

    public boolean isAll() {
        return type == CacheType.ALL;
    }

    public CacheType getType() {
        return type;
    }

    public long getSize() {
        return size;
    }

    public long getTtl() {
        return ttl;
    }
}
