package tutorial.spark.launcher

import org.apache.spark.launcher.SparkLauncher

import scala.io.Source

/**
  * Created by cloudwick on 6/25/2018.
  */
object SparkLauncherTest extends App{
  /*
  def main(args: Array[String]): Unit = {
    val launcher = new SparkLauncher()
      .setAppResource("/home/cloudera/Desktop/SparkExamples-1.0-SNAPSHOT.jar")
      .setMainClass("tutorial.spark.example.WordCount")
      .setMaster("local[*]")
      .setSparkHome("/usr/lib/spark/")
    //val res = launcher.startApplication()
    val res = launcher.launch()
    res.waitFor()
  }*/

  val spark = new SparkLauncher()
    .setSparkHome("/usr/lib/spark")
    .setAppResource("/home/cloudera/Abhishek/Programs/sparkexample_2.10-1.0.jar")
    .setMainClass("tutorial.spark.example.WordCount")
    .setMaster("local[*]")
    .setVerbose(true)
    .launch()
  spark.waitFor()
  val lines = Source.fromInputStream(spark.getInputStream).getLines()
  lines.foreach(println)
}
