/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.infinivision.flink.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.cli.Options;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlNodeInfo;
import org.apache.flink.table.api.QueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.functions.*;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.FunctionEntry;
import org.apache.flink.table.client.gateway.*;
import org.apache.flink.table.client.gateway.local.ExecutionContext;
import org.apache.flink.table.client.gateway.local.ProgramDeployer;
import org.apache.flink.table.client.gateway.local.ResultStore;
import org.apache.flink.table.client.gateway.local.result.BasicResult;
import org.apache.flink.table.client.gateway.local.result.ChangelogResult;
import org.apache.flink.table.client.gateway.local.result.DynamicResult;
import org.apache.flink.table.client.gateway.local.result.MaterializedResult;
import org.apache.flink.table.client.utils.SqlJobUtil;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FunctionDescriptor;
import org.apache.flink.table.util.TableSchemaUtil;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Executor that performs the Flink communication locally. The calls are blocking depending on the
 * response time to the Flink cluster. Flink jobs are not blocking.
 */
public class LocalExecutorExtend implements Executor {

    private static final Logger LOG = LoggerFactory.getLogger(LocalExecutorExtend.class);

    private static final String DEFAULT_ENV_FILE = "sql-client-defaults.yaml";

    private static final AtomicInteger TMP_VIEW_SEQUENCE_ID = new AtomicInteger(0);

    // deployment

    private final Environment defaultEnvironment;
    private final List<URL> dependencies;
    private final Configuration flinkConfig;
    private final List<CustomCommandLine<?>> commandLines;
    private final Options commandLineOptions;

    // result maintenance

    private final ResultStore resultStore;

    /**
     * Cached execution context for unmodified sessions. Do not access this variable directly
     * but through {@link LocalExecutorExtend#getOrCreateExecutionContext}.
     */
    private ExecutionContext<?> executionContext;

    /**
     * Creates a local executor for submitting table programs and retrieving results.
     */
    public LocalExecutorExtend(URL defaultEnv, List<URL> jars, List<URL> libraries) {
        // discover configuration
        final String flinkConfigDir;
        try {
            // find the configuration directory
            flinkConfigDir = CliFrontend.getConfigurationDirectoryFromEnv();

            // load the global configuration
            this.flinkConfig = GlobalConfiguration.loadConfiguration(flinkConfigDir);

            // initialize default file system
            try {
                FileSystem.initialize(this.flinkConfig);
            } catch (IOException e) {
                throw new SqlClientException(
                        "Error while setting the default filesystem scheme from configuration.", e);
            }

            // load command lines for deployment
            this.commandLines = CliFrontend.loadCustomCommandLines(flinkConfig, flinkConfigDir);
            this.commandLineOptions = collectCommandLineOptions(commandLines);
        } catch (Exception e) {
            throw new SqlClientException("Could not load Flink configuration.", e);
        }

        // try to find a default environment
        if (defaultEnv == null) {
            final String defaultFilePath = flinkConfigDir + "/" + DEFAULT_ENV_FILE;
            System.out.println("No default environment specified.");
            System.out.print("Searching for '" + defaultFilePath + "'...");
            final File file = new File(defaultFilePath);
            if (file.exists()) {
                System.out.println("found.");
                try {
                    defaultEnv = Path.fromLocalFile(file).toUri().toURL();
                } catch (MalformedURLException e) {
                    throw new SqlClientException(e);
                }
                LOG.info("Using default environment file: {}", defaultEnv);
            } else {
                System.out.println("not found.");
            }
        }

        // inform user
        if (defaultEnv != null) {
            System.out.println("Reading default environment from: " + defaultEnv);
            try {
                defaultEnvironment = Environment.parse(defaultEnv);
            } catch (IOException e) {
                throw new SqlClientException("Could not read default environment file at: " + defaultEnv, e);
            }
        } else {
            defaultEnvironment = new Environment();
        }

        // discover dependencies
        dependencies = discoverDependencies(jars, libraries);

        // prepare result store
        resultStore = new ResultStore(flinkConfig);
    }

    /**
     * Constructor for testing purposes.
     */
    public LocalExecutorExtend(Environment defaultEnvironment, List<URL> dependencies, Configuration flinkConfig, CustomCommandLine<?> commandLine) {
        this.defaultEnvironment = defaultEnvironment;
        this.dependencies = dependencies;
        this.flinkConfig = flinkConfig;
        this.commandLines = Collections.singletonList(commandLine);
        this.commandLineOptions = collectCommandLineOptions(commandLines);

        // prepare result store
        resultStore = new ResultStore(flinkConfig);
    }

    @Override
    public void start() {
    }

    @Override
    public Map<String, String> getSessionProperties(SessionContext session) throws SqlExecutionException {
        final Environment env = getOrCreateExecutionContext(session)
                .getMergedEnvironment();
        final Map<String, String> properties = new HashMap<>();
        properties.putAll(env.getExecution().asTopLevelMap());
        properties.putAll(env.getDeployment().asTopLevelMap());
        return properties;
    }

    @Override
    public List<String> listTables(SessionContext session) throws SqlExecutionException {
        final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
                .createEnvironmentInstance()
                .getTableEnvironment();
        return Arrays.asList(tableEnv.listTables());
    }

    @Override
    public List<String> listUserDefinedFunctions(SessionContext session) throws SqlExecutionException {
        final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
                .createEnvironmentInstance()
                .getTableEnvironment();
        return Arrays.asList(tableEnv.listUserDefinedFunctions());
    }

    @Override
    public void setDefaultDatabase(SessionContext session, String namePath) throws SqlExecutionException {
        final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
                .createEnvironmentInstance()
                .getTableEnvironment();

        try {
            tableEnv.setDefaultDatabase(namePath.split("\\."));
        } catch (Throwable t) {
            // catch everything such that the query does not crash the executor
            throw new SqlExecutionException("No catalog and database with this name could be found.", t);
        }
    }

    @Override
    public List<String> listCatalogs(SessionContext session) throws SqlExecutionException {
        final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
                .createEnvironmentInstance()
                .getTableEnvironment();
        return Arrays.asList(tableEnv.listCatalogs());
    }

    @Override
    public List<String> listDatabases(SessionContext session) throws SqlExecutionException {
        final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
                .createEnvironmentInstance()
                .getTableEnvironment();
        return Arrays.asList(tableEnv.listDatabases());
    }

    @Override
    public TableSchema getTableSchema(SessionContext session, String name) throws SqlExecutionException {
        final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
                .createEnvironmentInstance()
                .getTableEnvironment();
        try {
            return tableEnv.scan(name.split("\\.")).getSchema();
        } catch (Throwable t) {
            // catch everything such that the query does not crash the executor
            throw new SqlExecutionException("No table with this name could be found.", t);
        }
    }

    @Override
    public String explainStatement(SessionContext session, String statement) throws SqlExecutionException {
        final ExecutionContext<?> context = getOrCreateExecutionContext(session);
        final TableEnvironment tableEnv = context
                .createEnvironmentInstance()
                .getTableEnvironment();

        // translate
        try {
            final Table table = createTable(tableEnv, statement);
            // explanation requires an optimization step that might reference UDFs during code compilation
            return context.wrapClassLoader(() -> tableEnv.explain(table));
        } catch (Throwable t) {
            // catch everything such that the query does not crash the executor
            throw new SqlExecutionException("Invalid SQL statement.", t);
        }
    }

    @Override
    public List<String> completeStatement(SessionContext session, String statement, int position) {
        final TableEnvironment tableEnv = getOrCreateExecutionContext(session)
                .createEnvironmentInstance()
                .getTableEnvironment();

        try {
            // TODO: FLINK-8865
            //return Arrays.asList(tableEnv.getCompletionHints(statement, position));
            return Collections.emptyList();
        } catch (Throwable t) {
            // catch everything such that the query does not crash the executor
            if (LOG.isDebugEnabled()) {
                LOG.debug("Could not complete statement at " + position + ":" + statement, t);
            }
            return Collections.emptyList();
        }
    }

    @Override
    public ResultDescriptor executeQuery(SessionContext session, String query) throws SqlExecutionException {
        final ExecutionContext<?> context = getOrCreateExecutionContext(session);
        return executeQueryInternal(context, query);
    }

    @Override
    public TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(SessionContext session,
                                                                         String resultId) throws SqlExecutionException {
        final DynamicResult<?> result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
        }
        if (result.isMaterialized()) {
            throw new SqlExecutionException("Invalid result retrieval mode.");
        }
        return ((ChangelogResult<?>) result).retrieveChanges();
    }

    @Override
    public TypedResult<Integer> snapshotResult(SessionContext session, String resultId, int pageSize) throws SqlExecutionException {
        final DynamicResult<?> result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
        }
        if (!result.isMaterialized()) {
            throw new SqlExecutionException("Invalid result retrieval mode.");
        }
        return ((MaterializedResult<?>) result).snapshot(pageSize);
    }

    @Override
    public List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException {
        final DynamicResult<?> result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
        }
        if (!result.isMaterialized()) {
            throw new SqlExecutionException("Invalid result retrieval mode.");
        }
        return ((MaterializedResult<?>) result).retrievePage(page);
    }

    @Override
    public void cancelQuery(SessionContext session, String resultId) throws SqlExecutionException {
        final ExecutionContext<?> context = getOrCreateExecutionContext(session);
        cancelQueryInternal(context, resultId);
    }

    @Override
    public ProgramTargetDescriptor executeUpdate(SessionContext session, String statement) throws SqlExecutionException {
        final ExecutionContext<?> context = getOrCreateExecutionContext(session);
        return executeUpdateInternal(context, statement);
    }

    @Override
    public void validateSession(SessionContext session) throws SqlExecutionException {
        // throws exceptions if an environment cannot be created with the given session context
        getOrCreateExecutionContext(session).createEnvironmentInstance();
    }

    @Override
    public void createView(SessionContext session, String ddl) throws SqlExecutionException {
        final ExecutionContext<?> context = getOrCreateExecutionContext(session);
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        TableEnvironment tEnv = envInst.getTableEnvironment();

        try {
            List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(ddl);
            sqlNodeList
                    .stream()
                    .filter((node) -> node.getSqlNode() instanceof SqlCreateView)
                    .forEach((node) -> {
                        String subQuery = ((SqlCreateView) (node.getSqlNode())).getSubQuerySql();
                        Table view = tEnv.sqlQuery(subQuery);
                        List<String> aliasNames = ((SqlCreateView) node.getSqlNode()).getFieldNames();

                        if (!aliasNames.isEmpty()) {
                            String tmpView =
                                    "_tmp_" + ((SqlCreateView) node.getSqlNode()).getName() +
                                            "_" + TMP_VIEW_SEQUENCE_ID.incrementAndGet();
                            tEnv.registerTable(tmpView, view);

                            String viewSql = "SELECT * FROM" + tmpView;
                            StringBuilder aliasClause = new StringBuilder();
                            List<String> inputFields = view.getRelNode().getRowType().getFieldNames();
                            assert aliasNames.size() == inputFields.size();
                            if (aliasNames.size() != inputFields.size()) {
                                throw new RuntimeException("View definition and input fields not match: \nDef Fields: "
                                        + aliasNames.toString() + "\nInput Fields: " + inputFields.toString());
                            }

                            for (int idx = 0; idx < aliasNames.size(); ++idx) {
                                aliasClause.append("`" + inputFields.get(idx) + "` as `" + aliasNames.get(idx) + "`");
                                if (idx < aliasNames.size() - 1) {
                                    aliasClause.append(", ");
                                }
                            }
                            view = tEnv.sqlQuery(String.format(viewSql, aliasClause.toString()));
                        }

                        tEnv.registerTable(((SqlCreateView) node.getSqlNode()).getName(), view);
                    });
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a view from ddl: " + ddl, e);
        }
    }

    @Override
    public void createTable(SessionContext session, String ddl) throws SqlExecutionException {
        final ExecutionContext<?> context = getOrCreateExecutionContext(session);
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        TableEnvironment tEnv = envInst.getTableEnvironment();

        try {
            List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(ddl);
            sqlNodeList
                    .stream()
                    .filter((node) -> node.getSqlNode() instanceof SqlCreateTable)
                    .forEach((node) -> SqlJobUtil.registerExternalTable(tEnv, node));
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a table from ddl: " + ddl, e);
        }
    }

    @Override
    public void createFunction(SessionContext session, String ddl) throws SqlExecutionException {
        final ExecutionContext<?> context = getOrCreateExecutionContext(session);
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        TableEnvironment tEnv = envInst.getTableEnvironment();

        try {
            List<SqlNodeInfo> sqlNodeList = SqlJobUtil.parseSqlContext(ddl);
            sqlNodeList
                    .stream()
                    .filter((node) -> node.getSqlNode() instanceof SqlCreateFunction)
                    .forEach((node) -> {
                        SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) node.getSqlNode();
                        String funcName = sqlCreateFunction.getFunctionName().toString();
                        String funcDef = sqlCreateFunction.getClassName();

                        UserDefinedFunction func = createUserDefinedFunction(context.getClassLoader(), funcName, funcDef);

                        if (func instanceof TableFunction) {
                            TableFunction<?> tableFunction = (TableFunction) func;
                            tEnv.registerFunction(funcName, tableFunction);
                        } else if (func instanceof AggregateFunction) {
                            AggregateFunction<?, ?> aggregateFunction = (AggregateFunction) func;
                            tEnv.registerFunction(funcName, aggregateFunction);
                        } else if (func instanceof ScalarFunction) {
                            ScalarFunction scalarFunction = (ScalarFunction) func;
                            tEnv.registerFunction(funcName, scalarFunction);
                        } else {
                            // TODO: Support Hive UDX
                            throw new RuntimeException("Couldn't match the type of UDF class: " + funcDef);
                        }
                    });
        } catch (Exception e) {
            throw new SqlExecutionException("Could not create a udx from ddl: " + ddl, e);
        }
    }

    @Override
    public void stop(SessionContext session) {
        resultStore.getResults().forEach((resultId) -> {
            try {
                cancelQuery(session, resultId);
            } catch (Throwable t) {
                // ignore any throwable to keep the clean up running
            }
        });
    }

    // --------------------------------------------------------------------------------------------

    private <T> void cancelQueryInternal(ExecutionContext<T> context, String resultId) {
        final DynamicResult<T> result = resultStore.getResult(resultId);
        if (result == null) {
            throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
        }

        // stop retrieval and remove the result
        LOG.info("Cancelling job {} and result retrieval.", resultId);
        result.close();
        resultStore.removeResult(resultId);

        // stop Flink job
        try (final ClusterDescriptor<T> clusterDescriptor = context.createClusterDescriptor()) {
            ClusterClient<T> clusterClient = null;
            try {
                // retrieve existing cluster
                clusterClient = clusterDescriptor.retrieve(context.getClusterId());
                try {
                    clusterClient.cancel(new JobID(StringUtils.hexStringToByte(resultId)));
                } catch (Throwable t) {
                    // the job might has finished earlier
                }
            } catch (Exception e) {
                throw new SqlExecutionException("Could not retrieve or create a cluster.", e);
            } finally {
                try {
                    if (clusterClient != null) {
                        clusterClient.shutdown();
                    }
                } catch (Exception e) {
                    // ignore
                }
            }
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            throw new SqlExecutionException("Could not locate a cluster.", e);
        }
    }

    private <C> ProgramTargetDescriptor executeUpdateInternal(ExecutionContext<C> context, String statement) {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();
        applyUpdate(context, envInst.getTableEnvironment(), envInst.getQueryConfig(), statement);
        return null;
    }

    private <C> ResultDescriptor executeQueryInternal(ExecutionContext<C> context, String query) {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();

        // create table
        final Table table = createTable(envInst.getTableEnvironment(), query);

        // initialize result
        final DynamicResult<C> result = resultStore.createResult(
                context.getMergedEnvironment(),
                removeTimeAttributes(table.getSchema()),
                envInst.getExecutionConfig());

        // create job graph with dependencies
        final String jobName = context.getSessionContext().getName() + ": " + query;
        final JobGraph jobGraph;
        try {
            // createJobGraph requires an optimization step that might reference UDFs during code compilation
            jobGraph = context.wrapClassLoader(() -> {
                table.writeToSink(result.getTableSink(), envInst.getQueryConfig());
                return envInst.createJobGraph(jobName);
            });
        } catch (Throwable t) {
            // the result needs to be closed as long as
            // it not stored in the result store
            result.close();
            // catch everything such that the query does not crash the executor
            throw new SqlExecutionException("Invalid SQL query.", t);
        }

        // store the result with a unique id (the job id for now)
        final String resultId = jobGraph.getJobID().toString();
        resultStore.storeResult(resultId, result);

        // create execution
        final ProgramDeployer<C> deployer = new ProgramDeployer<>(
                context, jobName, jobGraph, result, true);

        // start result retrieval
        result.startRetrieval(deployer);

        return new ResultDescriptor(
                resultId,
                removeTimeAttributes(table.getSchema()),
                result.isMaterialized());
    }

    public <T> void modify(SessionContext session, String requestBody) throws Exception {
        final ExecutionContext<T> context = (ExecutionContext<T>) getOrCreateExecutionContext(session);
        final ClusterDescriptor<T> clusterDescriptor = context.createClusterDescriptor();
        final T clusterId = context.getClusterId();
        ClusterClient<T> clusterClient = clusterDescriptor.retrieve(clusterId);

        JSONObject request = (JSONObject)JSON.parse(requestBody);
        JobID jobId = JobID.fromHexString((String)request.get("jobid"));
        Map<String,String> vertexs = (Map<String,String>)request.get("vertex-parallelism-resource");

        List<JobVertexID> jobVertexIDs =vertexs.keySet().stream().map((vertexId) -> {
            return JobVertexID.fromHexString(vertexId);
        }).collect(Collectors.toList());

        List<Integer> parallelisms = vertexs.values().stream().map((parallelism) -> {
            return Integer.parseInt(parallelism);
        }).collect(Collectors.toList());

        try {
            CompletableFuture<Acknowledge> rescaleFuture = clusterClient.updateJob(
                    jobId,
                    jobVertexIDs,
                    parallelisms);
            rescaleFuture.get();
        } finally {
            try {
                if (clusterClient != null) {
                    clusterClient.shutdown();
                }
            } catch (Exception e) {
                // ignore
            }
        }

    }

    public <T> JobStatus getJobStatus(SessionContext session, String requestBody) throws Exception {
        final ExecutionContext<T> context = (ExecutionContext<T>) getOrCreateExecutionContext(session);
        final ClusterDescriptor<T> clusterDescriptor = context.createClusterDescriptor();
        final T clusterId = context.getClusterId();
        ClusterClient<T> clusterClient = clusterDescriptor.retrieve(clusterId);

        JSONObject request = (JSONObject)JSON.parse(requestBody);
        JobID jobId = JobID.fromHexString((String)request.get("jobid"));

        try {
            CompletableFuture<JobStatus> resutlFuture = clusterClient.getJobStatus(jobId);
            return resutlFuture.get();
        } finally {
            try {
                if (clusterClient != null) {
                    clusterClient.shutdown();
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     *
     * @param session
     * @param statement
     * @return
     */
    public ProgramTargetDescriptor commitJob(SessionContext session, String statement) throws SqlExecutionException {
        final ExecutionContext<?> context = getOrCreateExecutionContext(session);
        return commitJobInternal(context, statement);
    }

    private <C>  ProgramTargetDescriptor commitJobInternal(ExecutionContext<C> context, String statement) {
        final ExecutionContext.EnvironmentInstance envInst = context.createEnvironmentInstance();

        // create job graph with dependencies
        final String jobName = context.getSessionContext().getName() + ": " + statement;
        final JobGraph jobGraph;
        try {
            // createJobGraph requires an optimization step that might reference UDFs during code compilation
            jobGraph = context.wrapClassLoader(() -> {
                return envInst.createJobGraph(jobName);
            });
        } catch (Throwable t) {
            // catch everything such that the statement does not crash the executor
            throw new SqlExecutionException("Invalid SQL statement.", t);
        }

        // create execution
        final BasicResult<C> result = new BasicResult<>();
        final ProgramDeployer<C> deployer = new ProgramDeployer<>(
                context, jobName, jobGraph, result, false);

        // blocking deployment
        deployer.run();

        return ProgramTargetDescriptor.of(
                result.getClusterId().toString(),
                jobGraph.getJobID(),
                result.getWebInterfaceUrl());
    }

    /**
     * Creates a table using the given query in the given table environment.
     */
    private Table createTable(TableEnvironment tableEnv, String selectQuery) {
        // parse and validate query
        try {
            return tableEnv.sqlQuery(selectQuery);
        } catch (Throwable t) {
            // catch everything such that the query does not crash the executor
            throw new SqlExecutionException("Invalid SQL statement.", t);
        }
    }

    /**
     * Applies the given update statement to the given table environment with query configuration.
     */
    private <C> void applyUpdate(ExecutionContext<C> context, TableEnvironment tableEnv, QueryConfig queryConfig, String updateStatement) {
        // parse and validate statement
        try {
            // update statement requires an optimization step that might reference UDFs during code compilation
            context.wrapClassLoader(() -> {
                tableEnv.sqlUpdate(updateStatement, queryConfig);
                return null;
            });
        } catch (Throwable t) {
            // catch everything such that the statement does not crash the executor
            throw new SqlExecutionException("Invalid SQL update statement.", t);
        }
    }

    /**
     * Creates or reuses the execution context.
     */
    public synchronized ExecutionContext<?> getOrCreateExecutionContext(SessionContext session) throws SqlExecutionException {
        if (executionContext == null || !executionContext.getSessionContext().equals(session)) {
            try {
                executionContext = new ExecutionContext<>(defaultEnvironment, session, dependencies,
                        flinkConfig, commandLineOptions, commandLines);
            } catch (Throwable t) {
                // catch everything such that a configuration does not crash the executor
                throw new SqlExecutionException("Could not create execution context.", t);
            }
        }
        return executionContext;
    }

    // --------------------------------------------------------------------------------------------

    private static List<URL> discoverDependencies(List<URL> jars, List<URL> libraries) {
        final List<URL> dependencies = new ArrayList<>();
        try {
            // find jar files
            for (URL url : jars) {
                JobWithJars.checkJarFile(url);
                dependencies.add(url);
            }

            // find jar files in library directories
            for (URL libUrl : libraries) {
                final File dir = new File(libUrl.toURI());
                if (!dir.isDirectory()) {
                    throw new SqlClientException("Directory expected: " + dir);
                } else if (!dir.canRead()) {
                    throw new SqlClientException("Directory cannot be read: " + dir);
                }
                final File[] files = dir.listFiles();
                if (files == null) {
                    throw new SqlClientException("Directory cannot be read: " + dir);
                }
                for (File f : files) {
                    // only consider jars
                    if (f.isFile() && f.getAbsolutePath().toLowerCase().endsWith(".jar")) {
                        final URL url = f.toURI().toURL();
                        JobWithJars.checkJarFile(url);
                        dependencies.add(url);
                    }
                }
            }
        } catch (Exception e) {
            throw new SqlClientException("Could not load all required JAR files.", e);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Using the following dependencies: {}", dependencies);
        }

        return dependencies;
    }

    private static Options collectCommandLineOptions(List<CustomCommandLine<?>> commandLines) {
        final Options customOptions = new Options();
        for (CustomCommandLine<?> customCommandLine : commandLines) {
            customCommandLine.addGeneralOptions(customOptions);
            customCommandLine.addRunOptions(customOptions);
        }
        return CliFrontendParser.mergeOptions(
                CliFrontendParser.getRunCommandOptions(),
                customOptions);
    }

    private static TableSchema removeTimeAttributes(TableSchema schema) {
        return TableSchemaUtil.withoutTimeAttributes(schema);
    }

    /**
     * Create user defined function.
     */
    private static UserDefinedFunction createUserDefinedFunction(ClassLoader classLoader, String funcName, String funcDef) {
        DescriptorProperties properties = new DescriptorProperties();
        properties.putString("name", funcName);
        properties.putString("from", "class");
        properties.putString("class", funcDef);

        final FunctionDescriptor desc = FunctionEntry.create(properties).getDescriptor();

        return FunctionService.createFunction(desc, classLoader, false);
    }

}
