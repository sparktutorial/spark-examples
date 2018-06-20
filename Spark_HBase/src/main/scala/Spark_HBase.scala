import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import it.nerdammer.spark.hbase._

/**
  * Created by cloudwick on 6/19/2018.
  */
object Spark_HBase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HBase Spark Integration")
    val sc = new SparkContext(conf)
    val data = sc.hbaseTable[(String, String)]("temp").select("col1").inColumnFamily("columnfamily1")
    data.collect().foreach(println)
  }
}
