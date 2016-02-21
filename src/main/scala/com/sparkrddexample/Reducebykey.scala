package com.sparkrddexample


import org.apache.spark.SparkContext

import org.apache.spark.SparkConf

/**
 * Created by suri on 11/02/16.
 */
object Reducebykey {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.set("spark.app.name", "My Spark App")
    conf.set("spark.master", "local[4]")
    conf.set("spark.ui.port", "36000") // Override the default port
    // Create a SparkContext with this configuration
    val sc = new SparkContext(conf.setAppName("Reducebykey"))

     val d = List(( "panda", 0), ("pink", 3), ("pirate", 3), ("panda", 1), ("pink", 4))

    val input = sc.parallelize(d)
      //after map values we get = panda,(0,1) , pirate,(3,1)
      //then with reduce by key it will sum it.. here using map for k,v i.e.x,y    so for same keys lets say panda it will
      //(panda, (0,1)) ,  (panda,(1,1) here x._1 = 0, y._1 = 1, so thats equal to 1,
      //now x._2 = 1, y._2 = 1 so thats equal to 2 so its (panda, (1,2))
     // (x is reduction of previous 2 values and y is current value.)
    .mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).foreach(println)
  }

}
