package co.il.naya.kafka.tutorial2

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.google.common.collect.Lists
import com.twitter.hbc.core.{Client, Constants, Hosts, HttpHosts}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory


object TwitterProducer {
  val logger = LoggerFactory.getLogger(getClass)
  val consumerKey = "YocHAvophqW2U2nhZunGsaBKo"
  val consumerSecret = "925xHUYEDfvnswjnDM6a1yKuazxiT4WiAxKIMry8SXU1tdjLwI"
  val token = "2328601399-mfDuqVoBBiEJJaj8TzZH9Rjp4sbnIp7LVxfOs32"
  val tokenSecret = "7v4Jet3GTxStGPSrhkpW0sRwZX4yuQ03TpesJLyqBIOWo"

  // Optional: set up some followings and track terms
  val terms = Lists.newArrayList("kafka", "usa", "iai", "politics")

  def main(args: Array[String]): Unit = {
    TwitterProducer.run()

  }

  def run(): Unit = {
    logger.info("Setup")

    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue[String](1000)

    // create a twitter client
    val client: Client = createTwitterClient(msgQueue)
    //Attemps to establish
    client.connect()

    // create a kafka producer
    val producer: KafkaProducer[String, String] = createKafkaProducer()

    // add a shutdown hook// add a shutdown hook
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      def foo() = {
        logger.info("stopping application...")
        logger.info("shutting down client from twitter...")
        client.stop
        logger.info("closing producer...")
        producer.close
        logger.info("done!")
      }

      foo()
    }))


    // loop to  send tweets to kafka
    // on a different thread, or multiple different threads....// on a different thread, or multiple different threads....
    while (!client.isDone) {
      var msg: String = null
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS)
      }
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
          client.stop()
      }
      if (msg !=null){
        logger.info(msg)
        producer.send(new ProducerRecord[String, String]("twitter_tweets", null, msg), new Callback {
          override def onCompletion(metadata: RecordMetadata, e: Exception): Unit = {
            if (e!= null) {
              logger.error("Something bad happened", e);
            }
          }
        })
      }
    logger.info("End of application")
    }
  }




  def createTwitterClient(msgQueue: BlockingQueue[String]): Client ={
    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    val hosebirdHosts = new HttpHosts(Constants.STREAM_HOST)
    val hosebirdEndpoint = new StatusesFilterEndpoint

    hosebirdEndpoint.trackTerms(terms)

    // These secrets should be read from a config file
    val hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret)

    val builder = new ClientBuilder()
      .name("Hosebird-Client-01")
      .hosts(hosebirdHosts) // optional: mainly for the logs
      .authentication(hosebirdAuth)
      .endpoint(hosebirdEndpoint)
      .processor(new StringDelimitedProcessor(msgQueue)) // optional: use this if you want to process client events

    val hosebirdClient = builder.build()

    hosebirdClient
  }

  def createKafkaProducer():KafkaProducer[String, String]={
    val bootstrapServers = "127.0.0.1:9092"

    // create Producer properties
    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // create safe Producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5") // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

    // high throughput producer (at the expense of a bit of latency and CPU usage)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)) // 32 KB batch size

    // create the producer
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    return producer
  }

}
