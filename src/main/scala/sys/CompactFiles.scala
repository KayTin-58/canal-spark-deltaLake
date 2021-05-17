package kaytin.data.sys

import com.alibaba.fastjson.JSON

object CompactFiles {

  def main(args: Array[String]): Unit = {
    var json = ""
    if (args != null && args.length >= 1) {
      json = args(0)
    }
    val settings = scala.collection.mutable.HashMap("ifPartition" -> "true", "partitionValue" -> "20210409",
      "path" -> "/user/dsdfds/spark/delta-tableoms_order/oms_order")
    compactFiles(initSettings(json))
  }


  def initSettings(json: String) = {
    val settings = scala.collection.mutable.HashMap[String, String]()
    val jsonObject = JSON.parseObject(json)
    val ifPartition = jsonObject.getString("ifPartition")
    val partitionValue = jsonObject.getString("partitionValue")
    val path = jsonObject.getString("path")
    settings.+=("ifPartition" -> ifPartition)
    settings.+=("partitionValue" -> partitionValue)
    settings.+=("path" -> path)
  }


  def compactFiles(setting: scala.collection.mutable.HashMap[String, String]): Unit = {
    val spark = RealTimeDataSys.getSpark(scala.collection.mutable.HashMap())
    val path = setting.get("path").get
    val numFilesPerPartition = setting.getOrElse("numFilesPerPartition ", "16").toInt
    val ifPartition = setting.getOrElse("ifPartition", "false").toBoolean
    var partitionValue = setting.getOrElse("partitionValue", "1970-01-01")
    // 分区的情况
    if (ifPartition) {
      partitionValue = s"dt == '${partitionValue}'"
      spark.read
        .format("delta")
        .load(path)
        .where(partitionValue)
        .repartition(numFilesPerPartition)
        .write
        .option("dataChange", "false")
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", partitionValue)
        .save(path)
    } else {
      // 不分区的情况
      spark.read
        .format("delta")
        .load(path)
        .repartition(numFilesPerPartition)
        .write
        .option("dataChange", "false")
        .format("delta")
        .mode("overwrite")
        .save(path)
    }
  }
}
