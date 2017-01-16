import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object RddToDataFrame {

  // Case Class to be used as the DataFrame schema
  case class Word(word: String)

  def main(args: Array[String]): Unit = {


    // In Spark 2.0 SparkSession includes also the SparkContext and SqlContext

    // Create SparkSession
    val spark = SparkSession
      .builder
      .appName("OpMining")
      .config("spark.master", "local")
      .getOrCreate()


    // Read the data from text.txt, split on spaces and map over case class
    val data = spark.sparkContext.textFile("file:///text.txt")
      .flatMap(_.split(" "))
      .map(w => Word(w))

    println("***********************************************************************************************")
    
    Print the contents of RDD
    data.collect().foreach(println)

    println("***********************************************************************************************")

    // Convert the RDD to DataFrame type
    val df = spark.createDataFrame(data).toDF("word")
    
    // Print the dataframe contents
    df.show()

    println("***********************************************************************************************")

    spark.stop()

  }
}
