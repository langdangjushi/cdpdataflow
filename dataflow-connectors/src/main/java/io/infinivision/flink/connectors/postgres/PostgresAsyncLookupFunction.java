package io.infinivision.flink.connectors.postgres;

import io.infinivision.flink.connectors.CacheableFunction;
import io.infinivision.flink.connectors.cache.CacheBackend;
import io.infinivision.flink.connectors.utils.CacheConfig;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOptions;
import org.apache.flink.api.java.io.jdbc.JDBCOptions;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.functions.AsyncTableFunction;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.util.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PostgresAsyncLookupFunction extends AsyncTableFunction<BaseRow> implements CacheableFunction<List<Object>, List<BaseRow>> {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresAsyncLookupFunction.class);

    private final TableProperties tableProperties;
    private final RowType returnType;
    private final String queryTemplate;
    private Vertx vertx;
    private transient SQLClient SQLClient;
    private final CacheConfig cacheConfig;
    private transient CacheBackend<List<Object>, List<BaseRow>> cache;

    private final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;

    // QUERY TIMEOUT 必须小于 AsyncWaitOperator record entry的timeout时间
    // 目前 AsyncWaitOperator timeout = 10s
    // 否则 JDBCConnection不会被释放
    private final static int DEFAULT_QUERY_TIMEOUT = 5;

    private final static int DEFAULT_VERTX_WORKER_POOL_SIZE = Math.min(Runtime.getRuntime().availableProcessors(), 4) * 2;

    private final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public PostgresAsyncLookupFunction(TableProperties tableProperties,
                                       String queryTemplate,
                                       RowType returnType,
                                       CacheConfig cacheConfig) {
        this.tableProperties = tableProperties;
        this.queryTemplate = queryTemplate;
        this.returnType = returnType;
        this.cacheConfig = cacheConfig;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        JsonObject pgConfig = new JsonObject();
        pgConfig.put("url", tableProperties.getString(JDBCOptions.DB_URL))
                .put("user", tableProperties.getString(JDBCOptions.USER_NAME))
                .put("password", tableProperties.getString(JDBCOptions.PASSWORD))
                .put("driver_class", "org.postgresql.Driver")
                .put("max_pool_size", DEFAULT_MAX_DB_CONN_POOL_SIZE);

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(DEFAULT_VERTX_WORKER_POOL_SIZE);
        this.vertx = Vertx.vertx(vo);
        this.SQLClient = JDBCClient.createNonShared(vertx, pgConfig);
        LOG.info("open PostgresAsyncLookupFunction");
        LOG.info("Vertx configuration - max_pool_size: {}. default query timeout: {}",
                DEFAULT_MAX_DB_CONN_POOL_SIZE, DEFAULT_QUERY_TIMEOUT);
        if (this.cacheConfig.hasCache()) {
            this.cache = buildCache(context.getMetricGroup());
        }
    }


    public void eval(final ResultFuture<BaseRow> resultFuture, Object... values) {
        JsonArray inputParams = new JsonArray();
        for (Object value : values) {
            if (value instanceof BinaryString) {
                BinaryString input = (BinaryString) value;
                inputParams.add(input.toString());
            } else {
                inputParams.add(value);
            }
        }
        List<Object> cacheKey = inputParams.getList();

        if (this.cache != null && this.cache.exist(cacheKey)) {
            resultFuture.complete(this.cache.get(cacheKey));
            // cache hit
            return;
        }

        SQLClient.getConnection(conn -> {
            if (conn.failed()) {
                //Treatment failures
                LOG.error("Postgres Vertx Connection Failed: " + conn.cause());
                resultFuture.completeExceptionally(conn.cause());
                return;
            }
            final SQLConnection connection = conn.result()
                    .setOptions(new SQLOptions().setQueryTimeout(DEFAULT_QUERY_TIMEOUT));

            connection.queryWithParams(queryTemplate, inputParams, rs -> {
                if (rs.failed()) {
                    LOG.error("Postgres SQL Query Error: " + rs.cause());
                    throw new RuntimeException(rs.cause());
                }

                int resultSize = rs.result().getResults().size();
                GenericRow reuseRow = new GenericRow(returnType.getArity());
                List<BaseRow> results = new ArrayList<>();
                if (resultSize > 0) {
                    for (JsonArray line : rs.result().getResults()) {
                        for (int pos = 0; pos < reuseRow.getArity(); pos++) {
                            reuseRow.update(pos, line.getValue(pos));
                        }
                        results.add(GenericRow.copyReference(reuseRow));
                    }
                } else {
                    results = Collections.emptyList();
                }
                if (this.cache != null) {
                    this.cache.put(cacheKey, results);
                }
                resultFuture.complete(results);
                connection.close(done -> {
                    if (done.failed()) {
                        LOG.error("close connection failed: " + done.cause());
                        throw new RuntimeException(done.cause());
                    }
                });

            });
        });
        // sleep some time for sqlclient to get data, just test
//        try {
//            Thread.sleep(100);
//        } catch (InterruptedException ignored) { }
    }

    @Override
    public DataType getResultType(Object[] arguments, Class[] argTypes) {
        return this.returnType;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close PostgresAsyncLookupFunction");
        super.close();
        if (SQLClient != null) {
            LOG.info("Close SQLClient");

            SQLClient.close(done -> {
                if (done.failed()) {
                    throw new RuntimeException(done.cause());
                }
            });
        }

        if (vertx != null) {
            LOG.info("Close vertx");
            vertx.close(done -> {
                if (done.failed()) {
                    throw new RuntimeException(done.cause());
                }
            });
        }

    }

    @Override
    public CacheConfig getCacheConfig() {
        return this.cacheConfig;
    }

    @Override
    public List<BaseRow> loadValue(List<Object> key) {
        return null;
    }
}
