import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, ResultScanner, Scan}

//import scala.util.parsing.json.JSONObject
/*
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableInputFormat
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import it.nerdammer.spark.hbase._
*/
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

/**
  * Created by cloudwick on 6/19/2018.
  */
object Spark_HBase {
  def main(args: Array[String]): Unit = {
    /*
    val conf = new SparkConf().setAppName("HBase Spark Integration")
    val sc = new SparkContext(conf)
    val data = sc.hbaseTable[(String, String)]("temp").select("col1").inColumnFamily("columnfamily1")
    data.collect().foreach(println)
    */

    // Connection Establishment
    val config = HBaseConfiguration.create()
    config.set("hbase.zookeeper.quorum", "quickstart.cloudera")
    config.set("hbase.zookeeper.property.clientPort", "2181")

    val conn = ConnectionFactory.createConnection(config)

    val table = conn.getTable(TableName.valueOf("temp"))
    val scan = new Scan()
    val scanner = table.getScanner(scan)

    val iter = scanner.iterator()
    while (iter.hasNext) {
      val record = iter.next()
      println("Row Key :: " + Bytes.toString(record.getRow))
      println("columnfamily1:col1 :: " + Bytes.toString(record.getValue(Bytes.toBytes("columnfamily1"), Bytes.toBytes("col1"))))
      println("columnfamily1:col2 :: " + Bytes.toString(record.getValue(Bytes.toBytes("columnfamily1"), Bytes.toBytes("col2"))))
    }
  }
}