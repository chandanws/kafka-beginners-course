package co.il.naya.kafka.tutorial1

import java.time.Duration
import java.util
import java.util.Properties
import scala.util.control.Breaks._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

object ConsumerDemoAssignSeek {
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val prop = new Properties()

    // create consumer config
    val bootstrapServers = "127.0.0.1:9092"
    val topic = "first_topic"

    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // create consumer
    val consumer = new KafkaConsumer[String, String](prop)

    // assign and seek are mostly used to replay data or fetch a specific message

    // assign
    val partitionToReadFrom = new TopicPartition(topic,0)
    val offsetToReadFrom:Long = 15L
    consumer.assign(util.Arrays.asList(partitionToReadFrom))

    //seek
    consumer.seek(partitionToReadFrom, offsetToReadFrom)

    val numberOfMessagesToRead: Int = 5
    var keepOnReading:Boolean = true
    var numberOfMessagesReadSoFar = 0

    // poll for new data
    while (true){
     val records:ConsumerRecords[String, String] =  consumer.poll(Duration.ofMillis(100))//new in Kafka 2.1

      import scala.collection.JavaConversions._
      for (record:ConsumerRecord[String,String] <- records.iterator()){
        numberOfMessagesReadSoFar +=1
        logger.info("Key: " + record.key() + ", Value: " + record.value())
        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset())
        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead){
          keepOnReading = false // to exit the while loop
          break // to exit the for loop
        }
      }
    }
    logger.info("Exiting the application")
  }

}
