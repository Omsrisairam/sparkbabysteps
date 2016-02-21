package com.sparkrddexample

/**
 * Created by suri on 19/02/16.
 */
//Year,First Name,County,Sex,Count

import org.apache.spark.{SparkConf, SparkContext}

object AllNamesAvg {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.set("spark.app.name", "AllNamesAvg")
    conf.set("spark.master", "local[4]")
    conf.set("spark.ui.port", "36000") // Override the default port
    // Create a SparkContext with this configuration
    val sc = new SparkContext(conf.setAppName("SparkRDDExample"))
    var file = sc.textFile(args(0))
    if(args.length < 2){
      file = sc.textFile("/home/suri/Downloads/Intellij_workspace/SparkTele/resources/babynames.csv")
    }

    val splitRDD = file.map(line => line.split(","))
    //map(n => (n(1),n(4).toInt)).reduceByKey((v1,v2) => v1 + v2).collect
    //filter(x=>{var sum = 0; x._2.foreach(a=> {sum=sum+a});sum > 12})
    var fewColumns = splitRDD.map(x => (x(1), x(4)))

    //skipping header row which has column names
    val header = fewColumns.first()
    fewColumns = fewColumns.filter(x=>x!= header)
    val countByName = fewColumns.map { case (name, count) => (name, count.toInt) }.reduceByKey(_ + _).foreach(println)
    //    println("SUM     of sophia "+countByName)
    //    //total number of keys
    //    val noOfKeys = fewColumns.map { case (name, count) => (name, count.toInt) }.countByKey().map { case (name, count) => count }.sum
    //
    //    println("total numer of values " + noOfKeys)
    //    //computed average of sophia name
    //    val mean = countByName / noOfKeys
    //    println(mean)
  }
}
