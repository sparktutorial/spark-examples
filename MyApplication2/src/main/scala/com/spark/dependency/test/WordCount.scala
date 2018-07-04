package com.spark.dependency.test

import org.apache.spark.{SparkConf, SparkContext}



object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)

    val inputData = sc.textFile(input, 3)
    val finalResult = inputData.flatMap(x => x.split(",")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    finalResult.collect.foreach(println)

    finalResult.saveAsTextFile(output)
  }

  def tempMethod(list: List[Int]): String = {
    val result = list.reduce(_ + _)
    result.toString
  }
}
