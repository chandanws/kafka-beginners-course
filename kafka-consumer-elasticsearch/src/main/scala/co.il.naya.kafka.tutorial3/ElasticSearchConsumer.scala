package co.il.naya.kafka.tutorial3

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

object ElasticSearchConsumer {

  def createClient(): RestHighLevelClient ={
    // replace with your own credentials
    val hostname = "kafka-course-4699447506.eu-west-1.bonsaisearch.net"
    // localhost or bonsai url
    val username = "u1hr0098cx"
    // needed only for bonsai
    val password = "uodkj2br4f"

    // don't do this if you run a local ES
    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password))

    val builder = RestClient.builder(
      new HttpHost(hostname, 443, "https"))
        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
          override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder  = httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        })

    val client = new RestHighLevelClient(builder)

    client
  }
  def createConsumer(topic:String):KafkaConsumer[String, String] = {
    val prop = new Properties()

    // create consumer config
    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "kafka-demo-elasticsearch"

    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // create consumer
    val consumer = new KafkaConsumer[String, String](prop)
    consumer.subscribe(util.Arrays.asList(topic))

    consumer
  }
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    val client : RestHighLevelClient = createClient()

    val consumer: KafkaConsumer[String, String] = createConsumer("twitter_tweets")

    while (true){
      val records:ConsumerRecords[String, String] =  consumer.poll(Duration.ofMillis(100))//new in Kafka 2.1

      import scala.collection.JavaConversions._
      for (record:ConsumerRecord[String,String] <- records.iterator()){
        // where we insert data into ES

        val indexRequest:IndexRequest = new IndexRequest("twitter", "tweets").source(record.value(), XContentType.JSON)

        val indexResponse:IndexResponse = client.index(indexRequest, RequestOptions.DEFAULT)

        val id = indexResponse.getId
        logger.info(id)
        Thread.sleep(1000)
      }
    }

    // close the client connection
    //client.close()

  }

}
