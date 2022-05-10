import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.col

import com.datastax.oss.driver.api.core.uuid.Uuids // com.datastax.cassandra:cassandra-driver-core:4.0.0
import com.datastax.spark.connector._              // com.datastax.spark:spark-cassandra-connector_2.11:2.4.3

case class Air(
	lon:Double,
 	lat:Double,
	nom_dept:String,
	nom_com:String,
	insee_com:Int,
	nom_station:String,
	code_station:String,
	typologie :String,
	influence: String,
	nom_poll :String,
	id_poll_ue: Int,
	valeur :Double,
	unite :String,
	date_debut_string: String,
	date_fin_string :String,
	statut_valid: Int,
	code_epci: Int
)

object StreamHandler {
	def main(args: Array[String]) {

		// initialize Spark
		val spark = SparkSession
			.builder
			.appName("Stream Handler")
			.config("spark.cassandra.connection.host", "localhost")
			.getOrCreate()

		import spark.implicits._

		// read from Kafka
		val inputDF = spark
			.readStream
			.format("kafka") // org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5
			.option("kafka.bootstrap.servers", "localhost:9092")
			.option("subscribe", "weather")
			.load()

		// only select 'value' from the table,
		// convert from bytes to string
		val rawDF = inputDF.selectExpr("CAST(value AS STRING)").as[String]

		// split each row on comma, load it to the case class
		val expandedDF = rawDF.map(row => row.split(","))
			.map(row => Air(
				row(0).toDouble,row(1).toDouble,row(2),row(3),row(4).toInt,row(5),row(6),row(7),row(8),
				row(9),row(10).toInt,row(11).toDouble,row(12),row(13),row(14),row(15).toInt,row(16).toInt
			))	

		// create a dataset function that creates UUIDs
		val makeUUID = udf(() => Uuids.timeBased().toString)

		// add the UUIDs and renamed the columns
		// this is necessary so that the dataframe matches the 
		// table schema in cassandra
		val summaryWithIDs = expandedDF
			.withColumn("uuid", makeUUID())
			.withColumn("date_debut",to_timestamp(col("date_debut_string")))
			.withColumn("date_fin",to_timestamp(col("date_fin_string")))
			.drop(col("date_debut_string"))
			.drop(col("date_fin_string"))

		// write dataframe to Cassandra
		val query = summaryWithIDs
			.writeStream
			.trigger(Trigger.ProcessingTime("5 seconds"))
			.foreachBatch { (batchDF: DataFrame, batchID: Long) =>
				println(s"Writing to Cassandra $batchID")
				batchDF
					.write
					.cassandraFormat("weather", "bigd") // table, keyspace
					.mode("append")
					.save()
			}
			.outputMode("update")
			.start()

		// until ^C
		query.awaitTermination()
	}
}
