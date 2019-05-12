package tutorial.spark.example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cloudwick on 6/17/2018.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    /*
    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val input = args(0)
    val output = args(1)

    val inputData = sc.textFile(input, 3)
    val finalResult = inputData.flatMap(x => x.split(",")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    finalResult.collect.foreach(println)

    finalResult.saveAsTextFile(output)
    */

    val conf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(conf)

    val input = "/user/cloudera/input1.txt"//args(0)
    val output = "/user/cloudera/Output1"///args(1)

    val inputData = sc.textFile(input, 3)
    //val inputData  = sc.parallelize(list, 2)
    val finalResult = inputData.flatMap(x => x.split(",")).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    finalResult.collect.foreach(println)

    finalResult.coalesce(1).saveAsTextFile(output)
  }
}
