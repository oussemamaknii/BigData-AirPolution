import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._

case class DeviceData(device:String, temp:Double, humd:Double, pres:Double)

object StreamHandler {
    def main(args: Array[String]) {

        val spark = SparkSession
        .builder
        .appName("Stream Handler")
        .getOrCreate()

        import spark.implicits._
        
        val inputDF = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "weather")
            .load()

        val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

        val expandedDF = rawDF.map(row => row.split(","))
           .map(row => DeviceData(
           row(1),
           row(2).toDouble,
           row(3).toDouble,
           row(4).toDouble
           ))

        val summaryDf = expandedDF
            .groupBy("device")
            .agg(avg("temp"), avg("humd"),avg("pres"))

        val query = summaryDf
            .writeStream
            .outputMode("update")
            .format("console")
            .start()

        query.awaitTermination()
    }
}