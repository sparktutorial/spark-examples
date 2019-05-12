/*
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cloudwick on 6/20/2018.
  */
object TestSparkApplication {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestSparkApplication")
    val sc = new SparkContext(sparkConf)
    val list = List(1,2,3,4,5,6,7,8,9,10)
    val rdd = sc.parallelize(list, 3)

    println("Filtered Even Numbers :: ")
    rdd.filter(x => x % 2 == 0).collect().foreach(println)
    val sum = rdd.reduce(_ + _)
    println("Sum of the Numbers :: " + sum)

  }
}
*/