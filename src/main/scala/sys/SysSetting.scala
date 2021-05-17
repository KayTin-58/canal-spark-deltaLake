package kaytin.data.sys

import java.util

import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

class SysSetting extends Serializable {
  @BeanProperty
  var kafkaBootstrapServers: String = _
  @BeanProperty
  var subscribeTopic: String = _
  @BeanProperty
  var consumerGroupId: String = _
  @BeanProperty
  var tablePrimerKeys: util.HashMap[String, String] = _
  @BeanProperty
  var deltaPath: String = _

}


object SysSetting {
  /*def main(args: Array[String]): Unit = {
    val sysSetting = new SysSetting
    sysSetting.setSubscribeTopic("example")
    sysSetting.setConsumerGroupId("canal.spark.1001")
    sysSetting.setKafkaBootstrapServers("10.210.40.55:8083")
    sysSetting.setDeltaPath("D:\\temp\\spark\\delta-table")
    println(JSON.toJSON(sysSetting))
  }*/
}