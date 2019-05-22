package io.infinivision.flink.connectors.jdbc

import java.lang.{Boolean => JBool}
import java.util.{Set => JSet}

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, TableSinkBase, UpsertStreamTableSink}
import org.apache.flink.table.util.{Logging, TableConnectorUtil}
import org.apache.flink.types.Row

abstract class JDBCTableSink(
  outputFormat: JDBCBaseOutputFormat)
  extends TableSinkBase[JTuple2[JBool, Row]]
    with UpsertStreamTableSink[Row]
    with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]]
    with Logging {

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    dataStream.addSink(new JDBCTableSinkFunction(outputFormat))
      .name(TableConnectorUtil.generateRuntimeName(getClass, getFieldNames))
  }

  override def emitBoundedStream(boundedStream: DataStream[JTuple2[JBool, Row]]): DataStreamSink[_] = {
    boundedStream.addSink(new JDBCTableSinkFunction(outputFormat))
      .name(TableConnectorUtil.generateRuntimeName(getClass, getFieldNames))
  }



  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes, getFieldNames)


}


abstract class JDBCTableSinkBuilder {
    protected var userName: String = _
    protected var password: String = _
    protected var driverName: String = _
    protected var driverVersion: String = _
    protected var dbURL: String = _
    protected var tableName: String = _
    protected var primaryKeys: Option[JSet[String]] = None
    protected var uniqueKeys: Option[JSet[JSet[String]]] = None
    protected var schema: Option[RichTableSchema] = None
    protected var updateMode: String = _

    def userName(userName: String): JDBCTableSinkBuilder = {
      this.userName = userName
      this
    }

    def password(password: String): JDBCTableSinkBuilder = {
      this.password = password
      this
    }

    def driverName(driverName: String): JDBCTableSinkBuilder = {
      this.driverName = driverName
      this
    }

    def driverVersion(driverVersion: String): JDBCTableSinkBuilder = {
      this.driverVersion = driverVersion
      this
    }

    def dbURL(dbURL: String): JDBCTableSinkBuilder = {
      this.dbURL = dbURL
      this
    }

    def tableName(tableName: String): JDBCTableSinkBuilder = {
      this.tableName = tableName
      this
    }

    def primaryKeys(primaryKeys: Option[JSet[String]]): JDBCTableSinkBuilder = {
      this.primaryKeys = primaryKeys
      this
    }

    def uniqueKeys(uniqueKeys: Option[JSet[JSet[String]]]): JDBCTableSinkBuilder = {
      this.uniqueKeys = uniqueKeys
      this
    }

    def schema(schema: Option[RichTableSchema]): JDBCTableSinkBuilder = {
      this.schema = schema
      this
    }

    def updateMode(updateMode: String): JDBCTableSinkBuilder = {
      this.updateMode = updateMode
      this
    }

    def build(): JDBCTableSink

}