package co.il.naya.kafka.tutorial1

import java.time.Duration
import java.util
import java.util.{Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

object ConsumerDemo {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val prop = new Properties()

    // create consumer config
    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "my-fourth-application"
    val topic = "first_topic"

    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // create consumer
    val consumer = new KafkaConsumer[String, String](prop)

    // subscribe consumer to our topic(s)
    consumer.subscribe(util.Arrays.asList(topic))

    // poll for new data
    while (true){
     val records:ConsumerRecords[String, String] =  consumer.poll(Duration.ofMillis(100))//new in Kafka 2.1

      import scala.collection.JavaConversions._
      for (record:ConsumerRecord[String,String] <- records.iterator()){
        logger.info("Key: " + record.key() + ", Value: " + record.value())
        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset())
      }
    }
  }

}
