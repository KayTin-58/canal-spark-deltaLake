package kaytin.data.sys

import java.time.format.DateTimeFormatter
import java.util
import java.util.{Collections, Optional}

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import io.delta.tables.DeltaTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, LocalDate}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.concurrent.duration.DurationInt

object RealTimeDataSys {

  val KAFKA_SERVERS = "kafkaBootstrapServers"
  val TOPIC = "subscribeTopic"
  val TABLE_PRIMER_KEY = "tablePrimerKey"
  val CONSUMER_GROUP_ID = "consumerGroupId"
  val DELTA_PATH = "deltaPath"
  val opTypes = Array("UPDATE", "INSERT", "DELETE")

  def main(args: Array[String]): Unit = {
    var json: String = "{\"consumerGroupId\":\"canal.spark.1002\",\"subscribeTopic\":\"kayTin\",\"kafkaBootstrapServers\":\"xxxxx:xxxx\",\"spark/deltaPath\":\"delta-table\"}"
    if (null != args && args.length >= 1) {
      json = args(0)
    }
    println("==================>" + json)
    val settings = initSettings(json)
    processData(settings)
    // continuousPro
    // read
    //continuousPro
    // batchReadFromKafka
  }


  def read = {
    val spark = getSpark(scala.collection.mutable.HashMap("p" -> "l"))
    val count = spark
      .read
      .format("delta")
      .load("/user/ffhfg/spark/delta-tableoms_order/oms_order")

      .repartition(32)

      .count
    println(count)
  }


  def initSettings(json: String) = {
    val settings = JSON.parseObject(json, new SysSetting().getClass)
    val settingMap = scala.collection.mutable.HashMap[String, String]()
    settingMap.+=(KAFKA_SERVERS -> settings.kafkaBootstrapServers)
    settingMap.+=(CONSUMER_GROUP_ID -> settings.consumerGroupId)
    settingMap.+=(TOPIC -> settings.subscribeTopic)
    settingMap.+=(DELTA_PATH -> settings.deltaPath)
    Optional.ofNullable(settings.tablePrimerKeys)
      .ifPresent(consumer => {
        consumer.forEach((k, v) => {
          settingMap.+=(k -> v)
        })
      })
    settingMap
  }


  /* def read(settings: scala.collection.mutable.HashMap[String, String]) = {
     val spark = getSpark(settings)
     /*val count = */spark.read.format("delta")
       .load("D:\\codeRep\\da-platform\\spark\\delta-tableoms_order\\oms_order")
       .where("dt = '20210402'")
       .explain(ExtendedMode.name)
     //println(count)
   }*/


  /** *
   * 核心数据处理模块
   *
   * @param settings 相关配置信息
   * @return void
   * @author KayTin
   * @date 2021/3/25
   */
  def processData(settings: scala.collection.mutable.HashMap[String, String]): Unit = {
    val spark = getSpark(settings)
    val kafkaBootServers = settings.get(KAFKA_SERVERS).get
    val kafkaTopic = settings.get(TOPIC).get
    val ifIncludeHeaders = settings.getOrElse("ifIncludeHeaders", "false")
    val startingOffsets = settings.getOrElse("startingOffsets", "latest") // earliest fro batch ;latest for streaming
    val kafkaDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootServers)
      .option("subscribe", kafkaTopic)
      .option("ifIncludeHeaders", ifIncludeHeaders)
      .option("startingOffsets", startingOffsets)
      .load()
    import spark.implicits._
    kafkaDF
      // 这里不关注 offset
      .selectExpr("CAST (value as STRING)")
      .as[String]
      .writeStream
      .trigger(Trigger.ProcessingTime(20.seconds))
      .foreachBatch {
        // every batch
        (df: Dataset[String], batchId: Long) =>
          println("BATCH_ID=>" + batchId)
          val binLogRecords = df.rdd.filter(l => {
            val canalJsonObject = JSON.parseObject(l)
            val opType = canalJsonObject.getString("type").toUpperCase
            opTypes.contains(opType)
          }).flatMap {
            line =>
              val canalJsonObject = JSON.parseObject(line)
              val rows = canalJsonObject.getJSONArray("data")
              val opType = canalJsonObject.getString("type").toUpperCase
              val db = canalJsonObject.getString("database")
              var tb = canalJsonObject.getString("table")
              val ts = canalJsonObject.getLong("ts")
              val sqlTypeObj = canalJsonObject.getJSONObject("sqlType")
              val mysqlTypeObj = canalJsonObject.getJSONObject("mysqlType")

              if (tb.endsWith("_new")) tb = tb.replace("_new", "")
              val pattern = "\\d+$".r
              var partitionValue = ""
              var ifPartition = false
              if (pattern.findAllIn(tb).hasNext) {
                ifPartition = true
                partitionValue = pattern.findAllIn(tb).next()
                tb = tb.replace(s"_${partitionValue}", "")
              }

              val primerKeyName = settings.getOrElse(s"${db}.${tb}.primerKey.name", "id")
              rows.asInstanceOf[JSONArray].asScala.map(v => {
                val row = v.asInstanceOf[JSONObject]
                val primerKeyValue = row.get(primerKeyName)
                val key = s"${db}\001${tb}\001${primerKeyValue}\001${opType}\001${partitionValue}"
                BinLogRecord(key, db, tb, ts, sqlTypeObj, mysqlTypeObj, row, opType, ifPartition, partitionValue, primerKeyName)
              }).groupBy(_.key)
                .map(records => {
                  // _1:key
                  // _2:list
                  val items = records._2.toSeq.sortBy(_.ts)
                  items.last
                })
          }
          val schemaSet = binLogRecords.map(record => {
            MysqlSchemaInfo(record.db, record.tb, record.sqlType, record.mysqlType, record.opType, record.ifPartition, record.partitionValue, record.primerKeyName)
          }).distinct()
            .groupBy(msi => msi.db + msi.tb + msi.opType + msi.partitionValue)
            .map(record => {
              val typeInfo = record._2.foldLeft((new JSONObject(), new JSONObject()))((jot, msi)
              => (jot._1.fluentPutAll(msi.sqlType), jot._2.fluentPutAll(msi.mysqlType)))
              val msi = record._2.head
              MysqlSchemaInfo(msi.db, msi.tb, typeInfo._1, typeInfo._2,
                msi.opType, msi.ifPartition, msi.partitionValue,
                msi.primerKeyName)
            }).map(msi => {
            val schema = JdbcTypeUtils.getMysqlStructType(msi.sqlType, msi.mysqlType)
            SchemaInfo(msi.db, msi.tb, schema, msi.opType, msi.ifPartition, msi.partitionValue, msi.primerKeyName)
          }).collect()
          schemaSet.foreach { table => {
            val tmpRDD = binLogRecords.filter(record => {
              record.db.equalsIgnoreCase(table.db) && record.tb.equalsIgnoreCase(table.tb) && record.opType.equalsIgnoreCase(table.opType) && record.partitionValue.equalsIgnoreCase(table.partitionValue)
            }).map(record => {
              var array = table.schema.fieldNames.map(record.row.getString(_))
              if (record.ifPartition) {
                val ifRowPartition = settings.getOrElse(s"${table.db}.${table.tb}.if_rowPartition", false).asInstanceOf[Boolean]
                //
                if (ifRowPartition) {
                  // 该模式下 默认为会有一个Long类型主键(暂时不启用)
                  val primerKeyValue = record.row.getString(table.primerKeyName).asInstanceOf[Long]
                  val temp = primerKeyValue / 2000000
                  array = array.:+(record.partitionValue + "-" + temp)
                } else {
                  // 补充分区值
                  array = array.:+(record.partitionValue)
                }
              }
              Row.fromSeq(array)
            })
            val stringTypeSchema = JdbcTypeUtils.changeStructTypeToStringType(table.schema, if (table.ifPartition) 1 else 0)
            val df = spark.createDataFrame(tmpRDD, stringTypeSchema)
            val primerKey = table.primerKeyName
            val partitionValue = table.partitionValue
            var conExpr = ""
            if ("UPDATE".equalsIgnoreCase(table.opType)) {
              // we have tow kinds update strategy that is deleteInsert and merge
              val updateStrategy = settings.getOrElse(s"${table.db}.${table.tb}.update.strategy", "DELETE_INSERT")
              if ("DELETE_INSERT".equalsIgnoreCase(updateStrategy)) {
                // 构造 where条件
                val list = new util.ArrayList[String](3)
                val localIterator = df.select(col(primerKey)).toLocalIterator()
                while (localIterator.hasNext) {
                  list.add(localIterator.next().getString(0))
                }
                if (table.ifPartition) {
                  if (list.size() == 1) {
                    //TODO
                    val ifRowPartition = settings.getOrElse(s"${table.db}.${table.tb}.if_rowPartition", false).asInstanceOf[Boolean]
                    if (ifRowPartition) {
                      val temp = list.get(0).asInstanceOf[Int] / 2000000
                      conExpr = s"dt == '${partitionValue}-${temp}' and ${primerKey} == '${list.get(0)}'"
                    }
                    conExpr = s"dt == '${partitionValue}' and ${primerKey} == '${list.get(0)}'"
                  } else {
                    // 一次删除多条数据 这种情况一般不存在
                  }
                } else {
                  if (list.size() == 1) {
                    conExpr = s"${table.primerKeyName} == '${list.get(0)}'"
                  }
                }
                println(s"delete condition:${conExpr}")
                // 删除旧的数据
                // 这里还有一种方案是利用delta 的merge功能，生成一张辅助表 在查询的时候做 原表和辅助表的merge
                val path = settings.get(DELTA_PATH).get + "/" + table.db + "/" + table.tb
                DeltaTable.forPath(spark, path).delete(conExpr)
              } else {
                // merge strategy

              }
            }
            val path = settings.get(DELTA_PATH).get + "/" + table.db + "/" + table.tb
            if (table.ifPartition) {
              df.write.format("delta").mode("append")
                .option("mergeSchema", "true")
                .option("mergeSchema", true)
                .partitionBy("dt")
                .save(path)
            } else {
              // 直接插入
              df.write.format("delta").mode("append")
                .option("mergeSchema", "true")
                .option("mergeSchema", true)
                .save(path)
            }
            // df.show()
            // df.select(col("*"),date_format(col(""),"yyyy-MM-dd") as "dt")
            // df.collect().foreach(data => println(s"${table.db}.${table.tb}:" + data))
          }
          }
      }.start().awaitTermination()
  }

  def getSpark(settings: scala.collection.mutable.HashMap[String, String]) = {
    val checkPointPath = settings.getOrElse("checkPointPath", "spark/checkPoint/dir")
    SparkSession.builder()
      .config("spark.sql.warehouse.dir", "spark/spark-warehouse/dir")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("checkpointLocation", checkPointPath)
      .config("spark.sql.streaming.metricsEnabled", "true")
      // 忽略作为source的delta的更新 改变
      .config("ignoreDeletes", true)
      .config("ignoreChanges", true)
      .master("local[3]")
      .getOrCreate()
  }

}


case class BinLogRecord(key: String = "",
                        db: String = "test",
                        tb: String = "test",
                        ts: Long,
                        sqlType: JSONObject,
                        mysqlType: JSONObject,
                        row: JSONObject,
                        opType: String,
                        ifPartition: Boolean,
                        partitionValue: String,
                        primerKeyName: String)


case class MysqlSchemaInfo(db: String = "test",
                           tb: String = "test",
                           sqlType: JSONObject,
                           mysqlType: JSONObject,
                           opType: String,
                           ifPartition: Boolean,
                           partitionValue: String,
                           primerKeyName: String)

case class DbInfo(db: String, tb: String, sqlType: String = "None", opType: String = "INSERT")

case class SchemaInfo(db: String, tb: String, schema: StructType,
                      opType: String = "INSERT",
                      ifPartition: Boolean = false,
                      partitionValue: String = "1997-01-01",
                      primerKeyName: String)

