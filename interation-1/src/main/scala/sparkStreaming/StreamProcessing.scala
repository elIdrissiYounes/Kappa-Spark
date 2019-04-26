package sparkStreaming
import org.elasticsearch.spark.sql._
import model.{ Activity, ActivityByProduct, VisitorsByProduct }
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{ Duration, Minutes, Seconds, StateSpec, StreamingContext }
import org.apache.spark.streaming.kafka010._
import utils.SparkUtils._
import functions._
import com.twitter.algebird.HyperLogLogMonoid
import config.Settings
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.elasticsearch.spark.streaming._

object StreamProcessing {
  def main(args: Array[String]): Unit = {
    // setup spark context
    val sc = getSparkContext("Lambda with Spark")
    val sqlContext = getSQLContext(sc)
    import sqlContext.implicits._

    val batchDuration = Seconds(2)

    def streamingApp(sc: SparkContext, batchDuration: Duration) = {
      val ssc = new StreamingContext(sc, batchDuration)
      val wlc = Settings.WebLogGen
      val topic = wlc.kafkaTopic

      val kafkaDirectParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "group.id"->"lambda",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer])

      val kafkaDirectStream = KafkaUtils.createDirectStream[String, String](
        ssc, PreferConsistent, Subscribe[String, String](Set(topic), kafkaDirectParams))

      //for converting the direct stream in the new version ;)
      val bb = kafkaDirectStream.map(record => (record.key, record.value))

      val activityStream = bb.transform(input => {
        input.flatMap { kv =>
          val line = kv._2
          val record = line.split("\\t")
          val MS_IN_HOUR = 1000 * 60 * 60
          if (record.length == 7)
            Some(Activity(record(0).toLong / MS_IN_HOUR * MS_IN_HOUR, record(1), record(2), record(3), record(4), record(5), record(6)))
          else
            None

        }
      })

      // save data to ES
      activityStream.foreachRDD { rdd =>
        val activityDF = rdd
          .toDF()
          .selectExpr("timestamp_hour", "referrer", "action", "prevPage", "page", "visitor", "product")

        activityDF.saveToEs("activity/docs")

      }

      // activity by product
      val activityStateSpec =
        StateSpec
          .function(mapActivityStateFunc)
          .timeout(Minutes(120))

      val statefulActivityByProduct = activityStream.transform(rdd => {
        val df = rdd.toDF()
        df.registerTempTable("activity")
        val activityByProduct = sqlContext.sql(
          """SELECT
              product,
              timestamp_hour,
              sum(case when action = 'purchase' then 1 else 0 end) as purchase_count,
              sum(case when action = 'add_to_cart' then 1 else 0 end) as add_to_cart_count,
              sum(case when action = 'page_view' then 1 else 0 end) as page_view_count
              from activity
              group by product, timestamp_hour """)
        activityByProduct
          .map(r=>((r.getString(0), r.getLong(1)),
            ActivityByProduct(r.getString(0), r.getLong(1), r.getLong(2), r.getLong(3), r.getLong(4))
          )).rdd

      }).mapWithState(activityStateSpec)

      val activityStateSnapshot = statefulActivityByProduct.stateSnapshots()
      activityStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)
        ) // only save or expose the snapshot every x seconds
        .map(sr => ActivityByProduct(sr._1._1, sr._1._2, sr._2._1, sr._2._2, sr._2._3))
      .saveToEs("abp/docs")



      //this is working fine
      // unique visitors by product
      val visitorStateSpec =
      StateSpec
        .function(mapVisitorsStateFunc)
        .timeout(Minutes(120))

      val statefulVisitorsByProduct = activityStream.map(a => {
        val hll = new HyperLogLogMonoid(12)
        ((a.product, a.timestamp_hour), hll(a.visitor.getBytes))
      }).mapWithState(visitorStateSpec)

      val visitorStateSnapshot = statefulVisitorsByProduct.stateSnapshots()
      visitorStateSnapshot
        .reduceByKeyAndWindow(
          (a, b) => b,
          (x, y) => x,
          Seconds(30 / 4 * 4)) // only save or expose the snapshot every x seconds
        .map(sr => VisitorsByProduct(sr._1._1, sr._1._2, sr._2.approximateSize.estimate))

        .saveToEs("vbp/visitprod")

      ssc
    }

    val ssc = getStreamingContext(streamingApp, sc, batchDuration)
    //ssc.remember(Minutes(5))
    ssc.start()
    ssc.awaitTermination()

  }

}
