package com.sparksql

import org.apache.spark.{SparkConf, SparkContext}

//// SQL statements can be run by using the sql methods provided by sqlContext.
//val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")
//
//// The results of SQL queries are DataFrames and support all the normal RDD operations.
//// The columns of a row in the result can be accessed by field index:
//teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
//
//// or by field name:
//teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)
//
//// row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
//teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
//// Map("name" -> "Justin", "age" -> 19)
object SparkSQLFirstSteps {

  case class Babies(year: String, first_name: String, county: String, sex: String, count: Int)

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.set("spark.app.name", "SparkSQLFirstSteps")
    conf.set("spark.master", "local[4]")
    conf.set("spark.ui.port", "36000") // Override the default port
    // Create a SparkContext with this configuration
    val sc = new SparkContext(conf.setAppName("SparkSQlFirstSteps"))

    // Create the SQLContext first from the existing Spark Context
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // Import statement to implicitly convert an RDD to a DataFrame
    import sqlContext.implicits._

   // var file = sc.textFile(args(0))
       //// if(args.length < 2){
        var file = sc.textFile("/home/suri/Downloads/Intellij_workspace/SparkTele/resources/babynames.csv")
      //  }

    val splitRDD = file.map(line => line.split(","))
    //map(n => (n(1),n(4).toInt)).reduceByKey((v1,v2) => v1 + v2).collect
    //filter(x=>{var sum = 0; x._2.foreach(a=> {sum=sum+a});sum > 12})
    // var fewColumns = splitRDD.map(x => (x(1), x(4)))

    //skipping header row which has column names
    val header = file.first()
    val fewColumns = file.filter(x => x != header)
    val splitDF = fewColumns.map(line => line.split(",")).map(p => Babies(p(0), p(1), p(2), p(3), p(4).toInt)).toDF()
    //map(n => (n(1),n(4).toInt)).reduceByKey((v1,v2) => v1 + v2).collect
    //filter(x=>{var sum = 0; x._2.foreach(a=> {sum=sum+a});sum > 12})
    splitDF.registerTempTable("babies")

    val namesCount = sqlContext.sql("select first_name, sum(count) from babies group By first_name order By first_name")
    namesCount.foreach(println)
  }
}