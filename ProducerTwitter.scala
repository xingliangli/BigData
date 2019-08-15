import java.util.concurrent.LinkedBlockingDeque
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

object ProducerTwitter {

  def main(args: Array[String]): Unit = {
    val queue = new LinkedBlockingDeque[Status](1000)
    val consumerKey = "yYAzSnDrxdSmSSyHfcZWaVyWG"
    val consumerSecret = "BGKuvVyYsPf8n3QzzJBRIOVEpU6iWedhYRVYPALCmqOyGNMnn3"
    val accessToken = "1158365448590778368-VTrKy5aq6aCJ4qjISBMA0F3pINnnWE"
    val accessTokenSecret = "2k3rq76RGYvw1sjhAIGFqlwhw3Uyk6tauFKYFczrzfUDQ"

    val topicName = "test"
    val keywords = "shooting"

    val confBuild = new ConfigurationBuilder()

    confBuild.setDebugEnabled(true)
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)


    val stream = new TwitterStreamFactory(confBuild.build()).getInstance()

    val listener = new StatusListener {

      override def onStatus(status: Status): Unit = {queue.offer(status)}

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {
        println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId)
      }

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {
        println("Got track limitation notice:" + numberOfLimitedStatuses)
      }

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {
        println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId)
      }

      override def onStallWarning(warning: StallWarning): Unit = {
        println("Got stall warning:" + warning)
      }

      override def onException(ex: Exception): Unit = {
        ex.printStackTrace()
      }
    }

    stream.addListener(listener)

    val query = new FilterQuery(keywords)

    stream.filter(query)

    /////Important Part Kafka Config ////

    val properties = new Properties()

    properties.put("metadata.broker.list","localhost:9092")
    properties.put("bootstrap.servers","localhost:9092")
    properties.put("ack","all")
    properties.put("retries","0")
    properties.put("batch.size","16384")
    properties.put("buffer.memory","33554432")
    properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String,String](properties)

    var count : Int = 0

    while(true){
      val status = queue.poll()
      if(status == null){
        Thread.sleep(100)
      }else {
        for (hashtagEntity <- status.getHashtagEntities) {
          println("Tweet" + status + " \nHashtag" + hashtagEntity)
          producer.send(new ProducerRecord[String, String](
            topicName, (count += 1).toString, status.getText()
          ))
        }
      }
    }

  }





}
