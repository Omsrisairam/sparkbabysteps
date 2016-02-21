package com.sparkrddexample


import org.apache.spark._

object PerKeyAvg {
  def main(args: Array[String]) {
    // Construct a conf
    val conf = new SparkConf()
    conf.set("spark.app.name", "PerKeyAvg")
    conf.set("spark.master", "local[4]")
    conf.set("spark.ui.port", "36000") // Override the default port
    // Create a SparkContext with this configuration
    val sc = new SparkContext(conf)
    val input = sc.parallelize(List(("coffee", 1), ("coffee", 2), ("panda", 4)))
    val result = input.combineByKey(
      //(1,1) (2,1) (4,1)
      (v) => (v, 1),
      //                            1               1+1
      (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      // Note: we could us mapValues here, but we didn't because it was in the next section
    ).map { case (key, value) => (key, value._1 / value._2.toFloat) }
    result.collectAsMap().map(println(_))
  }
}
