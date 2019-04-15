package kafka.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.CountDownLatch


object ConsumerDemoWithThread {
  def main(args: Array[String]): Unit = {
    new ConsumerDemoWithThread().run()
  }
}

class ConsumerDemoWithThread() {
  private def run(): Unit = {
    val logger = LoggerFactory.getLogger(getClass.getName)
    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "my-sixth-application"
    val topic = "first_topic"

    // latch for dealing with multiple threads
    val latch = new CountDownLatch(1)

    // create the consumer runnable
    logger.info("Creating the consumer thread")
    val myConsumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch)

    // start the thread
    val myThread = new Thread(myConsumerRunnable)
    myThread.start()

    // add a shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      def foo() = {
        logger.info("Caught shutdown hook")
        myConsumerRunnable.shutdown()
        try
          latch.await()
        catch {
          case e: InterruptedException =>
            e.printStackTrace()
        }
        logger.info("Application has exited")
      }

      foo()
    }))
    try
      latch.await()
    catch {
      case e: InterruptedException =>
        logger.error("Application got interrupted", e)
    } finally logger.info("Application is closing")
  }

  class ConsumerRunnable(val bootstrapServers: String, val groupId: String, val topic: String, var latch: CountDownLatch) extends Runnable { // create consumer configs
    val logger = LoggerFactory.getLogger(getClass.getName)

    val properties = new Properties
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // create consumer
    val consumer = new KafkaConsumer[String, String](properties)

    // subscribe consumer to our topic(s)
    consumer.subscribe(util.Arrays.asList(topic))

    override def run(): Unit = { // poll for new data
      try
          while ( {
            true
          }) {
            val records = consumer.poll(Duration.ofMillis(100)) // new in Kafka 2.0.0
            import scala.collection.JavaConversions._
            for (record <- records) {
              logger.info("Key: " + record.key + ", Value: " + record.value)
              logger.info("Partition: " + record.partition + ", Offset:" + record.offset)
            }
          }
      catch {
        case e: WakeupException =>
          logger.info("Received shutdown signal!")
      } finally {
        consumer.close()
        // tell our main code we're done with the consumer
        latch.countDown()
      }
    }

    def shutdown(): Unit = { // the wakeup() method is a special method to interrupt consumer.poll()
      // it will throw the exception WakeUpException
      consumer.wakeup()
    }
  }

}
