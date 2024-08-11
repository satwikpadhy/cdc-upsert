import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._


object main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("main").setLevel(Level.WARN)
    val log: Logger = org.apache.log4j.LogManager.getLogger("Spark ETL Log")
    log.setLevel(Level.WARN)

    val spark = SparkSession.builder().master("local[*]").appName("cdc_upsert").getOrCreate()
    spark.sparkContext.setLogLevel("OFF")

    val csvFile = args(0)

    val ip = args(1)
    val port = args(2)
    val database = args(3)
    val tablename = args(4)
    val user = args(5)
    val password = args(6)
    val primarykeys = args(7)
    val timestamp_keys = args(8)

    val start = System.currentTimeMillis

    //reading the source csv files
    var df = spark.read
      .option("sep",",")  //using commma as delimiter
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", "true")  // Assuming first row is header
      .option("inferSchema", "true") // Infers the schema of the DataFrame
      .csv(csvFile)

    df = df.toDF(df.columns.map(_.toLowerCase): _*)
    println( s"Source Count = ${df.count()}")

    df.createOrReplaceTempView("source")


    //selecting the latest row for each primary key from the source
    var updtDF = spark.sql(s"""
                 |select *
                 |from
                 |(
                 | select *,
                 | row_number() over(partition by ${primarykeys} order by ${timestamp_keys} desc) as rank
                 | from
                 | source
                 |)
                 |where rank = 1
       """.stripMargin).drop("rank")

    //Rows to be deleted from the target table
    val tbdDF = updtDF.filter( col("operation") === 'D')

    //Removing Delete operations from dataframe
    updtDF = updtDF.filter(col("operation") =!= 'D')


    println(s"Updated count = ${updtDF.count()}")

    //Reading the target table (before updation)
    var oldTableDF = spark.read.format("jdbc")
                          .option("url", s"jdbc:postgresql://${ip}:${port}/${database}")
                          .option("driver", "org.postgresql.Driver")
                          .option("dbtable", tablename)
                          .option("user", user)
                          .option("password", password)
                          .load()
    oldTableDF.persist()
    println(s"Old Table count: ${oldTableDF.count()}")
    oldTableDF =   oldTableDF.toDF(oldTableDF.columns.map(_.toLowerCase): _*)

    val pk = primarykeys.split(",").toSeq

    val deletedDF = oldTableDF.join(tbdDF, pk, "left_anti")
    println(s"After Deletion from old table count: ${deletedDF.count()}")
    val preFinalDF = deletedDF.join(updtDF, pk, "left_anti")
    println(s"count after removing updated rows: ${preFinalDF.count()}")
    val finalDF = preFinalDF.unionByName(updtDF)

    finalDF.persist()
    println(s"final df count = ${finalDF.count()}")
    println(s"final df distinct count = ${finalDF.distinct.count()}")

    finalDF.distinct
          .repartition(10)
          .write.mode("overwrite")
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", s"jdbc:postgresql://${ip}:${port}/${database}")
          .option("user", s"${user}")
          .option("truncate",true)
          .option("password", s"${password}")
          .option("dbtable", s"${tablename}")
          .save()

    val end = System.currentTimeMillis

    val time = end - start
    println(s"Time taken = $time ms")
    spark.stop()
  }
}