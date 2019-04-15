package co.il.naya.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object ProducerDemo {

  def main(args: Array[String]): Unit = {
    // create Producer properties
    val prop = new Properties()
    val bootstrapServers = "localhost:9092"
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    // create the producer
    val producer = new KafkaProducer[String, String](prop)

    // create producer record
    val producerRecord = new ProducerRecord[String, String]("first_topic", "hello world")

    // send data - asynchronous
    producer.send(producerRecord)

    // flush data
    producer.flush()

    // flush and close producer
    producer.close()
  }

}
