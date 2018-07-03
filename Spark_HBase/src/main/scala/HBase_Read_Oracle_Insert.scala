import java.sql.{DriverManager, SQLException}

import net.liftweb.json.{DefaultFormats, JArray, parse}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by cloudwick on 7/1/2018.
  */
object HBase_Read_Oracle_Insert {
  implicit val formats = DefaultFormats
  def main(args: Array[String]): Unit = {

    if(args.length != 6) {
      throw new RuntimeException("Invalid Number of Arguments...!!!")
    }

    val zookeeperList = args(0)
    val zookeeperPort = args(1)
    val hBaseNameSpace = args(2)
    val oracleJdbcUrl = args(3)
    val oracleUserName = args(4)
    val oraclePassword = args(5)
    //val batchSize = 100

    Class.forName("oracle.jdbc.driver.OracleDriver")
    val con = DriverManager.getConnection(oracleJdbcUrl,oracleUserName,oraclePassword)
    val stmt = con.createStatement()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", s"$zookeeperList")
    conf.set("hbase.zookeeper.property.clientPort", s"${zookeeperPort}")

    val conn = ConnectionFactory.createConnection(conf)

    val table: Table = conn.getTable(TableName.valueOf(s"$hBaseNameSpace", "job_status"))
    val scan: Scan = new Scan()
    val scanner: ResultScanner = table.getScanner(scan)

    val iter = scanner.iterator()
    var rowCount = 0L

    while (iter.hasNext) {
      val record = iter.next()
      val rowKey = Bytes.toString(record.getRow())
      //println("Row Key :: " + rowKey)
      val failMsg = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("fail_msg")))
      //println("f:fail_msg :: " + failMsg)
      val st = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("st")))
      //println("f:st :: " + st)
      val cfg = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("cfg")))
      //println("f:cfg :: " + cfg)
      val addTimeStamp = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("add_ts")))
      //println("f:add_ts :: " + addTimeStamp)
      val doneTimeStamp = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("done_ts")))
      //println("f:done_ts :: " + doneTimeStamp)

      val stepStatusUrl = Bytes.toString(record.getValue(Bytes.toBytes("f"), Bytes.toBytes("getStepStatusUrl")))
      val json = parse(stepStatusUrl)
      val finalStatus = (json \\ "status").children(0).extract[String]
      /*
      val allStepDetails = (json \\ "stepDetails").children
      var flag = true
      var count = 0
      for(steps <- allStepDetails) {
        val stepDetails = steps \\ classOf[JArray]
        stepDetails.foreach(x => {
          if(!x(0).toString.equalsIgnoreCase("completed")) {
            count = count + 1
          }
        })
      }

      val finalStatus = if(count == 0) "completed" else "failed"
      */
      //println("Final Status :: " + finalStatus)
      val insertQuery =
      s"""
         | INSERT INTO job_status (jobId, fail_msg, status, cfg, start_ts, end_ts, all_step_status, parent_col)
         | VALUES ('$rowKey', '$failMsg', '$st', '$cfg', '$addTimeStamp', '$doneTimeStamp', '$finalStatus', null)
         """.stripMargin
      //println(sqlStatement)
      stmt.execute(insertQuery)

      /*
      rowCount = rowCount + 1
      if(rowCount % batchSize == 0) {
        stmt.executeLargeBatch()
        stmt.clearBatch()
      }
      */

    }

    con.commit()
    if(con != null) {
      con.close()
    }
  }
}
