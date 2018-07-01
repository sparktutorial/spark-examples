import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

/**
  * Created by cloudwick on 7/1/2018.
  */
object HBase_Insert {
  def insertSampleRecord(
     rowId: String,
     columnFamily: String,
     col1: String,
     col2: String,
     value1: String,
     value2: String,
     hBaseTable: Table): Unit = {

    val p = new Put(Bytes.toBytes(rowId))
    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(col1), Bytes.toBytes(value1))
    p.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(col1), Bytes.toBytes(value2))

    hBaseTable.put(p)
  }

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "quickstart.cloudera")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val conn = ConnectionFactory.createConnection(conf)

    //val hTable = conn.getTable(TableName.valueOf("temp"))
    //insertSampleRecord("1234567890", "columnfamily1", "col1", "col2", "This is a sample record", "Sample Record", hTable)
    val hTable = conn.getTable(TableName.valueOf("job_status"))

    val rowKey = "2017081417524373999"
    val columnFamily = "f"
    val fail_msg = "This is a Second fail message"
    val st = "completed"
    val cfg = "This is a Second cfg message"
    val add_ts = "1505915734999"
    val done_ts = "1505915734999"
    val getStepStatusUrl =
      """
        | {
        |   "reportStatusDTOArray":[
        |      {
        |         "requestMadeTs":1530156172637,
        |         "notificationSent":true,
        |         "marketDefinition":"",
        |         "startedTs":1530156172637,
        |         "datafactzJson":"",
        |         "reportStatusId":2018062803225263763,
        |         "finishedTs":1530156969847,
        |         "stepDetails":{
        |            "data_cleansing":[
        |               "failed",
        |               "2018-06-28T03:28:09.292Z"
        |            ],
        |            "post_filters":[
        |               "completed",
        |               "2018-06-28T03:35:01.224Z"
        |            ],
        |            "rxprojection":[
        |               "completed",
        |               "2018-06-28T03:34:19.807Z"
        |            ],
        |            "basic_filters":[
        |               "completed",
        |               "2018-06-28T03:25:21.022Z"
        |            ],
        |            "ndv_count":[
        |               "completed",
        |               "2018-06-28T03:23:54.171Z"
        |            ],
        |            "p360data":[
        |               "completed",
        |               "2018-06-28T03:36:09.466Z"
        |            ],
        |            "stab_pat_elig":[
        |               "completed",
        |               "2018-06-28T03:32:01.035Z"
        |            ]
        |         },
        |         "status":"complete",
        |         "outputLocation":"",
        |         "outputTables":{
        |            "data_cleansing":[
        |               "prod_df2_pps_batch.rx_data_cleansing_2018062803225263763",
        |               4275385
        |            ],
        |            "post_filters":[
        |               "prod_df2_pps_batch.rx_post_filters_2018062803225263763",
        |               4275385
        |            ],
        |            "rxprojection":[
        |               "prod_df2_pps_batch.vw_rx_rxprojection_2018062803225263763",
        |               4275385
        |            ],
        |            "rxprojection_strata":[
        |               "prod_df2_pps_batch.rx_rxprojection_strata_2018062803225263763",
        |               401760
        |            ],
        |            "basic_filters":[
        |               "prod_df2_pps_batch.rx_basic_filters_2018062803225263763",
        |               4278518
        |            ],
        |            "p360data":[
        |               "prod_df2_pps_batch.rx_p360data_2018062803225263763",
        |               4275385
        |            ],
        |            "stab_pat_elig":[
        |               "prod_df2_pps_batch.rx_stab_pat_elig_2018062803225263763",
        |               4275385
        |            ]
        |         },
        |         "lastOutputTable":{
        |            "step":"p360data",
        |            "tableName":"prod_df2_pps_batch.rx_p360data_2018062803225263763",
        |            "count":4275385
        |         }
        |      }
        |   ]
        |}
      """.stripMargin

    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fail_msg"), Bytes.toBytes(fail_msg))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("st"), Bytes.toBytes(st))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cfg"), Bytes.toBytes(cfg))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("add_ts"), Bytes.toBytes(add_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("done_ts"), Bytes.toBytes(done_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("getStepStatusUrl"), Bytes.toBytes(getStepStatusUrl))

    hTable.put(put)
    println("Record inserted Successfully...!!!")
  }
}
