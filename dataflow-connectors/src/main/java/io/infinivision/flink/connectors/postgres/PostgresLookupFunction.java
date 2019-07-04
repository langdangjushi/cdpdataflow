package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.CacheableFunction;
import io.infinivision.flink.connectors.cache.CacheBackend;
import io.infinivision.flink.connectors.jdbc.BaseRowJDBCInputFormat;
import io.infinivision.flink.connectors.utils.CacheConfig;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PostgresLookupFunction extends TableFunction<BaseRow> implements CacheableFunction<List<Object>, List<BaseRow>> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresLookupFunction.class);

    private final BaseRowJDBCInputFormat inputFormat;
    private final RowType returnType;
    private final CacheConfig cacheConfig;
    private transient CacheBackend<List<Object>, List<BaseRow>> cache;
    // used for cache all
    private int[] lookupKeys;

    public PostgresLookupFunction(BaseRowJDBCInputFormat inputFormat, RowType returnType, CacheConfig cacheConfig, int[] lookupKeys) {
        this.inputFormat = inputFormat;
        this.returnType = returnType;
        this.cacheConfig = cacheConfig;
        this.lookupKeys = lookupKeys;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        inputFormat.openInputFormat();
        if (this.cacheConfig.hasCache()) {
            if (this.cacheConfig.isAll()) {
                this.cache = buildCache();
                loadData();
                ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
                executorService.scheduleAtFixedRate(this::loadData, cacheConfig.getTtl(), cacheConfig.getTtl(), TimeUnit.MILLISECONDS);
            } else {
                this.cache = buildCache(context.getMetricGroup());
            }
        }
    }

    public void eval(Object... values) throws IOException {
        LOG.debug("eval function. input values: %s", values);
        List<BaseRow> res;
        if (this.cache != null) {
            res = cache.get(Arrays.asList(values));
        } else {
            res = loadValue(Arrays.asList(values));
        }
        // even at cache_type=all, still cache may be not exist
        if (res != null && res.size() > 0)
            for (BaseRow row : res) {
                collect(row);
            }
    }


    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return this.returnType;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public CacheConfig getCacheConfig() {
        return this.cacheConfig;
    }

    private void loadData() {
        try {
            CacheBackend<List<Object>, List<BaseRow>> newCache = buildCache();
            inputFormat.open(null);
            BaseRow row = new GenericRow(returnType.getArity());
            while (true) {
                GenericRow r = (GenericRow) inputFormat.nextRecord(row);
                if (r == null) {
                    break;
                }
                List<Object> cacheKey = new ArrayList<>(lookupKeys.length);
                for (int i : lookupKeys) {
                    cacheKey.add(r.getField(i));
                }
                if (newCache.get(cacheKey) == null) {
                    newCache.put(cacheKey, new LinkedList<>());
                }
                List<BaseRow> rows = newCache.get(cacheKey);
                rows.add(GenericRow.copyReference(r));
            }
            // replace old cache
            this.cache = newCache;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<BaseRow> loadValue(List<Object> key) throws IOException {
        Object[][] queryParameters = new Object[][]{key.toArray()};
        inputFormat.setParameterValues(queryParameters);
        InputSplit[] inputSplits = inputFormat.createInputSplits(1);
        inputFormat.open(inputSplits[0]);
        BaseRow row = new GenericRow(returnType.getArity());
        List<BaseRow> res = new LinkedList<>();
        while (true) {
            BaseRow r = inputFormat.nextRecord(row);
            if (r == null) {
                break;
            }
            res.add(GenericRow.copyReference((GenericRow) r));
        }
        return res;
    }
}
