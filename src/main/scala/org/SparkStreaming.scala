package org
import org.apps._
import org.apps.cassandra.model.Movie
import org.apps.cassandra.model.Movie._
//import org.apps.cassandra.services.CassandraService
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apps.cassandra.services.MovieCassandraForeachWriter
class CreatingSparkObjects{

  def getSparkConf(): SparkConf ={
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setAppName("First Spark Application")
    sparkConf.setMaster("local[*]")
    sparkConf
  }
  def getSparkContext(sparkConf: SparkConf): SparkContext = {
    val sparkContext = new SparkContext(sparkConf)
    sparkContext
  }
  

}


object SparkStreaming {
  def run(): Unit = {
    val sparkObjs = new CreatingSparkObjects()
    val sparkConf = sparkObjs.getSparkConf()
    val sparkContext = sparkObjs.getSparkContext(sparkConf)

    val arrayRDD = sparkContext.parallelize(Array(1,2,3,4,5,6,7,8))
    println(arrayRDD.getClass)
    arrayRDD.foreach(println)
  }
  def db1(): Unit = {
    val conf=new SparkConf()
    conf.set("spark.master","local[*]")
    conf.set("spark.app.name","exampleApp")
    val sc= new SparkContext(conf)
    val spark = SparkSession
                   .builder
                   .appName("SQL example") //.master("local[*]")
                   .config("spark.sql.warehouse.dir", "/home/revit")
                   .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
                   .config("spark.cassandra.connection.host", "192.168.62.218")
                   .config("spark.cassandra.connection.port", "9042")
                   .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
                   .getOrCreate()
    spark.sql("SHOW NAMESPACES FROM history").show(false)
    val tableDf = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "total_traffic", "keyspace" -> "traffickeyspace"))
      .load()

    tableDf.show()


    spark.stop()
  }
  def createSparkSession(): SparkSession = {
       val warehouseLocation = "/home/revit"

       return SparkSession.builder()
              .master("spark://spark-master:7077") //.master("local[1]")
              .appName("SparkByExamples.com")
              .config("spark.sql.warehouse.dir", warehouseLocation)
              .config("spark.executor.memory", "1g")
              .config("spark.executor.cores", "1")
              .config("spark.driver.memory", "1g")
              .getOrCreate();
    }
  def kafkaProcessing(): Unit = {
       val conf=new SparkConf()
    conf.set("spark.master","local[*]")
    conf.set("spark.app.name","exampleApp")
    val sc= new SparkContext(conf)
    val spark = SparkSession
                   .builder
                   .appName("SQL example") //.master("local[*]")
                   .config("spark.sql.warehouse.dir", "/home/revit")
                   .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
                   .config("spark.cassandra.connection.host", "192.168.62.218")
                   .config("spark.cassandra.connection.port", "9042")
                   .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
                   .getOrCreate()
      import spark.implicits._
     
      val schema = StructType(Array(
        StructField("id", IntegerType),
        StructField("title", StringType),
        StructField("year", IntegerType),
        StructField("rating", DoubleType),
        StructField("duration", IntegerType)
      ))
      
      val movieDataset: Dataset[Movie] =  spark.readStream
                                  .format("kafka")
                                  .option("kafka.bootstrap.servers", "192.168.62.212:9092")
                                  .option("subscribe", "movie_topic")
                                  .option("startingOffsets", "latest") //.option("startingOffsets", "latest")
                                  .load()
                                  .selectExpr("CAST(value AS STRING)")
                                  .select(from_json(col("value"), movieSchema).as[Movie]
      )
      
     
       movieDataset
        .writeStream
        .foreach(new MovieCassandraForeachWriter(spark))
        .start()
        .awaitTermination()
                  
      /*
      val query = personDF.writeStream  
                           .outputMode("append") 
                           .format("console")  
                           .option("truncate", "False")  
                           .start()
                            .awaitTermination()
      */
      spark.stop()

  }
  def new_kafkaProcessing(): Unit = {
       val conf=new SparkConf()
    conf.set("spark.master","local[*]")
    conf.set("spark.app.name","exampleApp")
    val sc= new SparkContext(conf)
    val spark = SparkSession
                   .builder
                   .appName("SQL example") //.master("local[*]")
                   .config("spark.sql.warehouse.dir", "/home/revit")
                   .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
                   .config("spark.cassandra.connection.host", "192.168.62.218")
                   .config("spark.cassandra.connection.port", "9042")
                   .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
                   .getOrCreate()
      import spark.implicits._
     
      
      val movieDataset: Dataset[Movie]  = (spark.readStream.format("kafka")
                   .option("kafka.bootstrap.servers", "192.168.62.212:9092")
                   .option("subscribe", "movie_topic")
                   .option("startingOffsets", "latest")
                   .load()
                   .selectExpr("cast(value as string) as value")
                   .select(from_json(col("value"), movieSchema).as[Movie])
      )
      
      val query = movieDataset.writeStream  
                           .outputMode("append") 
                           .format("console")  
                           .option("truncate", "False")  
                           .start()
      
      query.awaitTermination()
      
      spark.stop()

  }
  def runSparkKafkaCassandra(): Unit = {
    //Creating spark session.
    val sparkSession = SparkSession.builder()
      .appName("Integrating Cassandra")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/home/revit")
      .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.connection.host", "192.168.62.218")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions","com.datastax.spark.connector.CassandraSparkExtension")
      .getOrCreate()

    //Instantiating RunJob then Reading and writing streams.
    //val runJob = new RunJob()
    //runJob.run(sparkSession)
  }
  def main(args: Array[String]): Unit = {
    //run()
    //db1()
    kafkaProcessing()
    //runSparkKafkaCassandra()
    //new_kafkaProcessing()
  }
}