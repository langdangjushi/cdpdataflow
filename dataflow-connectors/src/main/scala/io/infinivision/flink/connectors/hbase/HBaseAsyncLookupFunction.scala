package io.infinivision.flink.connectors.hbase

import java.lang.{Integer => JInteger}
import java.net.URI
import java.nio.file.{Files, StandardCopyOption}
import java.util
import java.util.Collections
import java.util.concurrent.{CompletableFuture, Executors, TimeUnit}

import com.stumbleupon.async.Callback
import io.infinivision.flink.connectors.CacheableFunction
import io.infinivision.flink.connectors.cache.CacheBackend
import io.infinivision.flink.connectors.utils.CacheConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple3 => JTuple3}
import org.apache.flink.connectors.hbase.table.HBaseTableSchemaV2
import org.apache.flink.connectors.hbase.util.HBaseBytesSerializer
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.runtime.security.{DynamicConfiguration, KerberosUtils}
import org.apache.flink.streaming.api.functions.async.ResultFuture
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.functions.{AsyncTableFunction, FunctionContext}
import org.apache.flink.table.dataformat.{BaseRow, BinaryString, GenericRow}
import org.apache.flink.table.util.{Logging, TableProperties}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HConstants
import org.hbase.async._

import scala.collection.JavaConverters._

class HBaseAsyncLookupFunction(
                                tableProperties: TableProperties,
                                tableSchema: RichTableSchema,
                                hbaseTableName: String,
                                hbaseSchema: HBaseTableSchemaV2,
                                rowKeySourceIndex: Int,
                                qualifierSourceIndexes: util.List[JInteger],
                                hbaseConfiguration: Configuration,
                                cacheConfig: CacheConfig)
  extends AsyncTableFunction[BaseRow]
    with CacheableFunction[util.List[Byte], util.List[BaseRow]]
    with Logging {

  private val qualifierList: util.List[JTuple3[Array[Byte], Array[Byte], TypeInformation[_]]] = hbaseSchema.getFamilySchema.getFlatByteQualifiers
  private val charset: String = hbaseSchema.getFamilySchema.getStringCharset
  private val inputFieldSerializers: util.List[HBaseBytesSerializer] = new util.ArrayList[HBaseBytesSerializer]()
  private val totalQualifiers: Int = hbaseSchema.getFamilySchema.getTotalQualifiers
  private var cache: CacheBackend[util.List[Byte], util.List[BaseRow]] = _


  //  private val serializedConfig: Array[Byte] = HBaseConfigurationUtil.serializeConfiguration(hbaseConfiguration)
  //  private val rowKeyInternalTypeIndex = HBaseTypeUtils.getTypeIndex(hbaseSchema.getRowKeyType)

  @transient private var hClient: HBaseClient = _

  for (index <- 0 to totalQualifiers) {
    if (index == rowKeySourceIndex) {
      inputFieldSerializers.add(new HBaseBytesSerializer(hbaseSchema.getRowKeyType, charset))
    } else {
      val typeInfo = if (index < rowKeySourceIndex) qualifierList.get(index) else qualifierList.get(index - 1)
      inputFieldSerializers.add(new HBaseBytesSerializer(typeInfo.f2, charset))
    }

  }

  def setupSecurityConfig(): Unit = {
    val priorConfig = javax.security.auth.login.Configuration.getConfiguration
    val currentConfig = new DynamicConfiguration(priorConfig)
    val loginContextName = tableProperties.getString(HBase121Validator.ASYNC_SASL_CLIENTCONFIG)
    var keyTabPath = tableProperties.getString(HBase121Validator.KEYTAB_PATH)
    val principal = tableProperties.getString(HBase121Validator.PRINCIPAL)
    // check the keytabPath
    LOG.info(s"AsyncHBaseLookup keytab: $keyTabPath, principal: $principal")
    val fs = FileSystem.get(URI.create(keyTabPath))
    if (!fs.exists(new Path(keyTabPath))) {
      throw new RuntimeException(s"AsyncHBaseLookup keyTabPath: $keyTabPath not exists")
    }
    if (fs.isDistributedFS) {
      val inputStream = fs.open(new Path(keyTabPath))
      val targetPath = Files.createTempFile("Flink-AsyncHBase-", ".keytab")
      Files.copy(inputStream, targetPath, StandardCopyOption.REPLACE_EXISTING)
      val localPath = targetPath.toUri.getPath
      LOG.info(s"HBaseAsyncLookupFunction KeyTab HDFS Remote $keyTabPath. LocalPath: $localPath")
      keyTabPath = localPath
      targetPath.toFile.deleteOnExit()
      inputStream.close()
    }

    currentConfig.addAppConfigurationEntry(loginContextName, KerberosUtils.keytabEntry(keyTabPath, principal))
    javax.security.auth.login.Configuration.setConfiguration(currentConfig)
  }

  override def open(context: FunctionContext): Unit = {
    LOG.info("start open HBaseAsyncLookupFunction...")
    super.open(context)

    LOG.info(s"HBaseAsyncLookupFunction TableProperties: $tableProperties")

    val isSecurityEnabled = tableProperties.getString(HBase121Validator.ASYNC_SECURITY_AUTH_ENABLE).toBoolean
    if (isSecurityEnabled) {
      setupSecurityConfig()
    }

    val asyncConfig = new Config()

    asyncConfig.overrideConfig(HConstants.ZOOKEEPER_QUORUM,
      tableProperties.getString(HConstants.ZOOKEEPER_QUORUM, "localhost"))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_SECURITY_AUTH_ENABLE.key(),
      tableProperties.getString(HBase121Validator.ASYNC_SECURITY_AUTH_ENABLE))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_SECURITY_AUTHENTICATION.key(),
      tableProperties.getString(HBase121Validator.ASYNC_SECURITY_AUTHENTICATION))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_KERBEROS_REGIONSERVER_PRINCIPAL.key(),
      tableProperties.getString(HBase121Validator.ASYNC_KERBEROS_REGIONSERVER_PRINCIPAL))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_RPC_PROTECTION.key(),
      tableProperties.getString(HBase121Validator.ASYNC_RPC_PROTECTION))

    asyncConfig.overrideConfig(HBase121Validator.ASYNC_SASL_CLIENTCONFIG.key(),
      tableProperties.getString(HBase121Validator.ASYNC_SASL_CLIENTCONFIG))

    LOG.info("=====Dump HBase Async Configuration=====")
    LOG.info(asyncConfig.dumpConfiguration())

    hClient = new HBaseClient(asyncConfig)
    if (cacheConfig.hasCache) {
      if (cacheConfig.isLRU) {
        this.cache = buildCache(context.getMetricGroup)
      } else {
        this.cache = buildCache()
        loadData()
        val executorService = Executors.newScheduledThreadPool(1)
        executorService.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = loadData()
        }, cacheConfig.getTtl, cacheConfig.getTtl, TimeUnit.MILLISECONDS)
      }
    }

    LOG.info("end open HBaseAsyncLookupFunction...")
  }

  override def close(): Unit = {
    LOG.info("start close HBaseAsyncLookupFunction...")

    super.close()

    if (null != hClient) {
      hClient.shutdown().join(5000)
    }
    LOG.info("end close HBaseAsyncLookupFunction...")

  }


  def parseResult(rowKey: AnyRef, cells: util.ArrayList[KeyValue]): GenericRow = {
    val reusedRow = new GenericRow(totalQualifiers + 1)
    reusedRow.update(rowKeySourceIndex, rowKey)
    cells.asScala.foreach(cell => {
      val family = cell.family()
      val qualifier = cell.qualifier()
      val qInfo = qualifierList.asScala.filter { qf =>
        util.Arrays.equals(family, qf.f0) && util.Arrays.equals(qualifier, qf.f1)
      }

      if (qInfo.isEmpty) {
        LOG.info(s"can not find family: ${Bytes.pretty(cell.family())}. qualifier: ${Bytes.pretty(cell.qualifier())}")
        throw new RuntimeException(s"can not find family: ${Bytes.pretty(cell.family())}. qualifier: ${Bytes.pretty(cell.qualifier())}")
      }

      if (qInfo.size != 1) {
        LOG.info(s"duplicated family: ${Bytes.pretty(cell.family())}. qualifier: ${Bytes.pretty(cell.qualifier())}")
        throw new RuntimeException(s"duplicated family: ${Bytes.pretty(cell.family())}. qualifier: ${Bytes.pretty(cell.qualifier())}")
      }
      val qualifierSrcIdx = qualifierSourceIndexes.get(qualifierList.indexOf(qInfo.head))
      reusedRow.update(qualifierSrcIdx, inputFieldSerializers.get(qualifierSrcIdx).fromHBaseBytes(cell.value()))

    })

    reusedRow
  }

  def eval(resultFuture: ResultFuture[BaseRow], rowKey: Object): Unit = {

    val rowKey2 = rowKey match {
      case _: BinaryString => rowKey.asInstanceOf[BinaryString].toString
      case _ => rowKey
    }

    val rk = inputFieldSerializers.get(rowKeySourceIndex).toHBaseBytes(rowKey2)
    // attention!! Array do not have equals method, it just inherits Object.equals
    // so need convert to List
    val cacheKey = util.Arrays.asList(rk: _*)
    if (this.cache != null && this.cache.exist(cacheKey)) {
      val rows = this.cache.get(cacheKey)
      // when cache type is all, rowkey is bytearray
      if (cacheConfig.isAll) {
        rows.asScala.foreach(e => e.asInstanceOf[GenericRow].update(rowKeySourceIndex, rowKey))
      }
      resultFuture.complete(rows)
      return
    } else if (this.cacheConfig.isAll) {
      // cache type is all, no need to search hbase again,just return empty
      resultFuture.complete(Collections.emptyList())
      return
    }
    val getRequest: GetRequest = new GetRequest(hbaseTableName, rk)
    val defered = hClient.get(getRequest)

    defered.addCallback[Unit](new Callback[Unit, util.ArrayList[KeyValue]] {
      override def call(cells: util.ArrayList[KeyValue]): Unit = {
        val result: util.List[BaseRow] =
          if (cells.size() == 0) {
            Collections.emptyList()
          } else {
            val row = parseResult(rowKey, cells)
            Collections.singletonList(row)
          }
        if (cache != null) {
          cache.put(cacheKey, result)
        }
        resultFuture.complete(result)
      }
    })
  }

  private def loadData() = {
    val scanner = hClient.newScanner(hbaseTableName)
    val defered = scanner.nextRows()
    val future = new CompletableFuture[Int]()
    defered.addCallback[Unit](new Callback[Unit, util.ArrayList[util.ArrayList[KeyValue]]] {
      override def call(rows: util.ArrayList[util.ArrayList[KeyValue]]): Unit = {
        val iter = rows.iterator()
        while (iter.hasNext) {
          val row = iter.next()
          val rk = row.get(0).key()
          cache.put(util.Arrays.asList(rk: _*), Collections.singletonList(parseResult(rk, row)))
        }
        future.complete(1)
      }
    })
    // wait until scan finished
    future.get()
  }

  /**
    * cache config : size, ttl, type, refreshInterval
    *
    * @return
    */
  override def getCacheConfig: CacheConfig = cacheConfig

  /**
    * get/compute a value of key
    *
    * @param key
    * @return
    */
  override def loadValue(key: util.List[Byte]): util.List[BaseRow] = ???
}
