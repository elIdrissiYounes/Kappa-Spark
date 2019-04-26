package utils


import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.{SparkContext, SparkConf}
//modified for yarn
//Next step is to test on discovery better performance maybe I will encounter problems of shuffling since there are few storage nodes there ....
object SparkUtils {
  def getSparkContext(appName: String) = {
    val checkpointDirectory = "/home/alienware/checkpointt"
    // get spark configuration
    val conf = new SparkConf()
      .setAppName(appName)
      .set("es.index.auto.create", "true")
      .set("es.nodes","localhost:9200")
      .setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    sc.setCheckpointDir(checkpointDirectory)
    sc
  }

  def getSQLContext(sc: SparkContext) = {
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext
  }

  def getStreamingContext(streamingApp: (SparkContext, Duration) => StreamingContext, sc: SparkContext, batchDuration: Duration) = {
    val creatingFunc = () => streamingApp(sc, batchDuration)
    val ssc =
      StreamingContext.getActiveOrCreate(creatingFunc)

    ssc
  }
}