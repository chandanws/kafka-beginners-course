package co.il.naya.kafka.tutorial1

import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory


// Run Kafka Console Consumer
//  bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic first_topic --group my-third-application
object ProducerDemoWithCallback {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // create Producer properties
    val prop = new Properties()
    val bootstrapServers = "127.0.0.1:9092"
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])


    // create the producer
    val producer = new KafkaProducer[String, String](prop)

    for (n <- 1 to 10) {
      // create producer record
      val producerRecord = new ProducerRecord[String, String]("first_topic", "hello world " + Integer.toString(n))

      // send data - asynchronous
      producer.send(producerRecord, new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          // executes every time a record is successfully sent or an exception is thrown
          if (e == null) {
            // the record was successfully sent
            logger.info("Received new metadata. \n" +
              "Topic: " + recordMetadata.topic() + "\n" +
              "Partition: " + recordMetadata.partition() + "\n" +
              "Offset: " + recordMetadata.offset() + "\n" +
              "Timestamp: " + recordMetadata.timestamp()
            )

          } else {
            logger.error("Error while producing", e)
          }
        }
      })
    }

    // flush data
    producer.flush()

    // flush and close producer
    producer.close()
  }

}
