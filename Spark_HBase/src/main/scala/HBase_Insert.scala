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
    //val hTable = conn.getTable(TableName.valueOf("job_status_new"))
    //val columnFamily = "f"

    val hTable = conn.getTable(TableName.valueOf("job_status_new"))
    val cf = "columnfamily1"

    val col1 = "col1"
    val col2 = "col2"
    val col3 = "col3"
    val col1Value = "TempArray"
    val col2Value = "ArrayList"
    val col3Value = List("one", "two", "three")

    //col3Value.

    /*
    val put = new Put(Bytes.toBytes("3"))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col1), Bytes.toBytes(col1Value))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col2), Bytes.toBytes(col2Value))
    //put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(col3), Bytes.toByteArrays(col3Value))

    hTable.put(put)
    */

    /*
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
    */

    val columnFamily = "f"
    val rowKey = "2018061913501847215"

    val add_ts = "2018-06-19T13:50:18.474Z"
    val cfg =
      """
        | { "job_parameters":{ "rxProjection":{ "includePrProjFlag":false, "patientageStrataPrimCol":null, "productGroup":null, "patientgenderStrataPrimCol":null, "key":221, "geographyStrataPri
        | mCol":null, "paytypeStrataPrimCol":null, "id":2928, "specialtyStrataPrimCol":null, "projectionMethod":"National",
        | "payerplanStrataPrimCol":null, "calculateProjectionFactorFlag":false, "studyId":2991 }, "flexibleOutputFields":[ {
        |  "columnName":"plan_id", "providerRelatedColumn":"", "context":"", "columnAlias":"plan_id", "providerRelatedTable"
        | :"", "patientHubColumnName":"plan_id", "externalColumnName":"", "externalTableName":"", "index":15 }, { "columnNam
        | e":"ims_payer_id", "providerRelatedColumn":"", "context":"", "columnAlias":"ims_payer_id", "providerRelatedTable":
        | "", "patientHubColumnName":"ims_payer_id", "externalColumnName":"", "externalTableName":"", "index":12 }, { "colum
        | nName":"fill_nbr", "providerRelatedColumn":"", "context":"", "columnAlias":"fill_nbr", "providerRelatedTable":"",
        | "patientHubColumnName":"fill_nbr", "externalColumnName":"", "externalTableName":"", "index":41 }, { "columnName":"
        | daw_cd", "providerRelatedColumn":"", "context":"", "columnAlias":"daw_cd", "providerRelatedTable":"", "patientHubC
        | olumnName":"daw_cd", "externalColumnName":"", "externalTableName":"", "index":34 }, { "columnName":"diag_cd", "pro
        | viderRelatedColumn":"", "context":"", "columnAlias":"diag_cd", "providerRelatedTable":"", "patientHubColumnName":"
        | diag_cd,diag_vers_typ_id", "externalColumnName":"", "externalTableName":"", "index":38 }, { "columnName":"supplier
        | _id", "providerRelatedColumn":"", "context":"", "columnAlias":"supplier_id", "providerRelatedTable":"", "patientHu
        | bColumnName":"supplier_id", "externalColumnName":"", "externalTableName":"", "index":45 }, { "columnName":"otlt_zi
        | p", "providerRelatedColumn":"", "context":"", "columnAlias":"otlt_zip", "providerRelatedTable":"", "patientHubColu
        | mnName":"otlt_zip", "externalColumnName":"", "externalTableName":"", "index":9 }, { "columnName":"usc_cd", "provid
        | erRelatedColumn":"", "context":"", "columnAlias":"usc_cd", "providerRelatedTable":"", "patientHubColumnName":"prod
        | uct_id", "externalColumnName":"product_id", "externalTableName":"v_product", "index":21 }, { "columnName":"mkted_p
        | rod_nm", "providerRelatedColumn":"", "context":"", "columnAlias":"mkted_prod_nm", "providerRelatedTable":"", "pati
        | entHubColumnName":"product_id", "externalColumnName":"product_id", "externalTableName":"v_product", "index":19 },
        | { "columnName":"product_id", "providerRelatedColumn":"", "context":"", "columnAlias":"product_id", "providerRelate
        | dTable":"", "patientHubColumnName":"product_id", "externalColumnName":"", "externalTableName":"", "index":18 }, {
        | "columnName":"ndc", "providerRelatedColumn":"", "context":"", "columnAlias":"ndc", "providerRelatedTable":"", "pat
        | ientHubColumnName":"ndc", "externalColumnName":"", "externalTableName":"", "index":20 }, { "columnName":"cmf_prod_
        | nbr", "providerRelatedColumn":"", "context":"", "columnAlias":"cmf_prod_nbr", "providerRelatedTable":"", "patientH
        | ubColumnName":"product_id", "externalColumnName":"product_id", "externalTableName":"v_product", "index":17 }, { "c
        | olumnName":"pharmacy_id", "providerRelatedColumn":"", "context":"", "columnAlias":"pharmacy_id", "providerRelatedT
        | able":"", "patientHubColumnName":"pharmacy_id", "externalColumnName":"", "externalTableName":"", "index":10 }, { "
        | columnName":"provider_id", "providerRelatedColumn":"provider_id", "context":"", "columnAlias":"provider_id", "prov
        | iderRelatedTable":"", "patientHubColumnName":"", "externalColumnName":"", "externalTableName":"v_provider", "index
        | ":23 }, { "columnName":"st_cd", "providerRelatedColumn":"st_cd", "context":"provider", "columnAlias":"provider_pro
        | vider_st_cd", "providerRelatedTable":"", "patientHubColumnName":"", "externalColumnName":"", "externalTableName":"
        | v_zip", "index":24 }, { "columnName":"zip", "providerRelatedColumn":"zip", "context":"provider", "columnAlias":"pr
        | ovider_zip", "providerRelatedTable":"", "patientHubColumnName":"", "externalColumnName":"", "externalTableName":"v
        | _zip", "index":25 }, { "columnName":"ims_rxer_id", "providerRelatedColumn":"ims_rxer_id", "context":"", "columnAli
        | as":"rxer_id", "providerRelatedTable":"", "patientHubColumnName":"", "externalColumnName":"", "externalTableName":
        | "v_provider", "index":22 }, { "columnName":"pat_gender_cd", "providerRelatedColumn":"", "context":"", "columnAlias
        | ":"pat_gender_cd", "providerRelatedTable":"", "patientHubColumnName":"pat_gender_cd", "externalColumnName":"", "ex
        | ternalTableName":"", "index":5 }, { "columnName":"pat_age_nbr", "providerRelatedColumn":"", "context":"", "columnA
        | lias":"pat_age_nbr", "providerRelatedTable":"", "patientHubColumnName":"pat_age_nbr", "externalColumnName":"", "ex
        | ternalTableName":"", "index":3 }, { "columnName":"patient_id", "providerRelatedColumn":"", "context":"", "columnAl
        | ias":"patient_id", "providerRelatedTable":"", "patientHubColumnName":"patient_id", "externalColumnName":"", "exter
        | nalTableName":"", "index":6 }, { "columnName":"pat_zip3_cd", "providerRelatedColumn":"", "context":"", "columnAlia
        | s":"pat_zip3_cd", "providerRelatedTable":"", "patientHubColumnName":"pat_zip3_cd", "externalColumnName":"", "exter
        | nalTableName":"", "index":7 }, { "columnName":"chnl_cd", "providerRelatedColumn":"", "context":"", "columnAlias":"
        | chnl_cd", "providerRelatedTable":"", "patientHubColumnName":"chnl_cd", "externalColumnName":"", "externalTableName
        | ":"", "index":8 }, { "columnName":"pharmacy_zip", "providerRelatedColumn":"", "context":"", "columnAlias":"pharmac
        | y_zip", "providerRelatedTable":"", "patientHubColumnName":"pharmacy_zip", "externalColumnName":"", "externalTableN
        | ame":"", "index":11 }, { "columnName":"pri_spcl_cd", "providerRelatedColumn":"pri_spcl_cd", "context":"", "columnA
        | lias":"provider_pri_spcl_cd", "providerRelatedTable":"", "patientHubColumnName":"", "externalColumnName":"", "exte
        | rnalTableName":"v_provider", "index":26 }, { "columnName":"pay_typ_cd", "providerRelatedColumn":"", "context":"",
        | "columnAlias":"pay_typ_cd", "providerRelatedTable":"", "patientHubColumnName":"pay_typ_cd", "externalColumnName":"
        | ", "externalTableName":"", "index":14 }, { "columnName":"rx_typ_cd", "providerRelatedColumn":"", "context":"", "co
        | lumnAlias":"rx_typ_cd", "providerRelatedTable":"", "patientHubColumnName":"rx_typ_cd", "externalColumnName":"", "e
        | xternalTableName":"", "index":42 }, { "columnName":"ims_pln_id", "providerRelatedColumn":"", "context":"", "column
        | Alias":"ims_pln_id", "providerRelatedTable":"", "patientHubColumnName":"ims_pln_id", "externalColumnName":"", "ext
        | ernalTableName":"", "index":13 }, { "columnName":"cmf_pack_nbr", "providerRelatedColumn":"", "context":"", "column
        | Alias":"cmf_pack_nbr", "providerRelatedTable":"", "patientHubColumnName":"cmf_pack_nbr", "externalColumnName":"",
        | "externalTableName":"", "index":16 }, { "columnName":"diag_vers_typ_id", "providerRelatedColumn":"", "context":"",
        |  "columnAlias":"diag_vers_typ_id", "providerRelatedTable":"", "patientHubColumnName":"diag_vers_typ_id", "external
        | ColumnName":"", "externalTableName":"", "index":39 }, { "columnName":"auth_rfll_nbr", "providerRelatedColumn":"",
        | "context":"", "columnAlias":"auth_rfll_nbr", "providerRelatedTable":"", "patientHubColumnName":"auth_rfll_nbr", "e
        | xternalColumnName":"", "externalTableName":"", "index":27 }, { "columnName":"bin", "providerRelatedColumn":"", "co
        | ntext":"", "columnAlias":"bin", "providerRelatedTable":"", "patientHubColumnName":"bin", "externalColumnName":"",
        | "externalTableName":"", "index":28 }, { "columnName":"claim_id", "providerRelatedColumn":"", "context":"", "column
        | Alias":"claim_id", "providerRelatedTable":"", "patientHubColumnName":"claim_id", "externalColumnName":"", "externa
        | lTableName":"", "index":29 }, { "columnName":"copay_amt", "providerRelatedColumn":"", "context":"", "columnAlias":
        | "copay_amt", "providerRelatedTable":"", "patientHubColumnName":"copay_amt", "externalColumnName":"", "externalTabl
        | eName":"", "index":30 }, { "columnName":"cust_prc", "providerRelatedColumn":"", "context":"", "columnAlias":"cust_
        | prc", "providerRelatedTable":"", "patientHubColumnName":"cust_prc", "externalColumnName":"", "externalTableName":"
        | ", "index":31 }, { "columnName":"dacon_qty", "providerRelatedColumn":"", "context":"", "columnAlias":"dacon_qty",
        | "providerRelatedTable":"", "patientHubColumnName":"dacon_qty", "externalColumnName":"", "externalTableName":"", "i
        | ndex":32 }, { "columnName":"days_supply_cnt", "providerRelatedColumn":"", "context":"", "columnAlias":"days_supply
        | _cnt", "providerRelatedTable":"", "patientHubColumnName":"days_supply_cnt", "externalColumnName":"", "externalTabl
        | eName":"", "index":33 }, { "columnName":"dspnsd_qty", "providerRelatedColumn":"", "context":"", "columnAlias":"dsp
        | nsd_qty", "providerRelatedTable":"", "patientHubColumnName":"dspnsd_qty", "externalColumnName":"", "externalTableN
        | ame":"", "index":35 }, { "columnName":"month_id", "providerRelatedColumn":"", "context":"", "columnAlias":"month_i
        | d", "providerRelatedTable":"", "patientHubColumnName":"month_id", "externalColumnName":"", "externalTableName":"",
        |  "index":36 }, { "columnName":"pat_pay_amt", "providerRelatedColumn":"", "context":"", "columnAlias":"pat_pay_amt"
        | , "providerRelatedTable":"", "patientHubColumnName":"pat_pay_amt", "externalColumnName":"", "externalTableName":""
        | , "index":37 }, { "columnName":"rx_dosage_amt", "providerRelatedColumn":"", "context":"", "columnAlias":"rx_dosage
        | _amt", "providerRelatedTable":"", "patientHubColumnName":"rx_dosage_amt", "externalColumnName":"", "externalTableN
        | ame":"", "index":40 }, { "columnName":"rx_written_dt", "providerRelatedColumn":"", "context":"", "columnAlias":"rx
        | _written_dt", "providerRelatedTable":"", "patientHubColumnName":"rx_written_dt", "externalColumnName":"", "externa
        | lTableName":"", "index":43 }, { "columnName":"svc_dt", "providerRelatedColumn":"", "context":"", "columnAlias":"sv
        | c_dt", "providerRelatedTable":"", "patientHubColumnName":"svc_dt", "externalColumnName":"", "externalTableName":""
        | , "index":44 }, { "columnName":"week_id", "providerRelatedColumn":"", "context":"", "columnAlias":"week_id", "prov
        | iderRelatedTable":"", "patientHubColumnName":"week_id", "externalColumnName":"", "externalTableName":"", "index":4
        | 7 }, { "columnName":"total_paid_amt", "providerRelatedColumn":"", "context":"", "columnAlias":"total_paid_amt", "p
        | roviderRelatedTable":"", "patientHubColumnName":"total_paid_amt", "externalColumnName":"", "externalTableName":"",
        |  "index":46 }, { "columnName":"cohort_desc", "providerRelatedColumn":"", "context":"", "columnAlias":"cohort_desc"
        | , "providerRelatedTable":"", "patientHubColumnName":"", "externalColumnName":"", "externalTableName":"", "index":1
        |  }, { "columnName":"cohort_id", "providerRelatedColumn":"", "context":"", "columnAlias":"cohort_id", "providerRela
        | tedTable":"", "patientHubColumnName":"", "externalColumnName":"", "externalTableName":"", "index":2 }, { "columnNa
        | me":"patient_eligibility", "providerRelatedColumn":"", "context":"", "columnAlias":"patient_eligibility", "provide
        | rRelatedTable":"", "patientHubColumnName":"", "externalColumnName":"", "externalTableName":"", "index":48 }, { "co
        | lumnName":"stable_pharmacy", "providerRelatedColumn":"", "context":"", "columnAlias":"stable_pharmacy", "providerR
        | elatedTable":"", "patientHubColumnName":"", "externalColumnName":"", "externalTableName":"", "index":49 }, { "colu
        | mnName":"pat_brth_yr_nbr", "providerRelatedColumn":"", "context":"", "columnAlias":"pat_brth_yr_nbr", "providerRel
        | atedTable":"", "patientHubColumnName":"pat_brth_yr_nbr", "externalColumnName":"", "externalTableName":"", "index":
        | 4 } ], "isTheClientNonPharmaAndNonMedTechFlag":false, "encumbering":{ "retainClaimsFailingSoBConcomitFlag":true, "
        | payEncumberingFlag":false, "providerEncumbering":"NO_ENCUMBERING" }, "outputConfigurations":{ "providerEncumbering
        | Type":1, "outputColumnsFields":[ ], "dirtyFlag":false, "key":222, "id":2911, "payTypeCleansingFlag":false, "fields
        | ClaimExtractType":1, "limitToSelectionPeriodOnlyFlag":false, "outputMetricsFields":[ ], "retainClaimsFailingSoBCon
        | comitFlag":true, "outputLayout":1, "flatFileFlag":false, "studyId":2991, "outputFields":[ { "udaFilterKey":null, "
        | entityId":4, "fieldId":65, "context":"Plan (Rx): Plan ID", "udaFilterUniqueId":null, "sortIndex":15, "id":291324,
        | "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Plan (Rx): Plan ID", "studyId":2991 }, { "udaFilterKe
        | y":null, "entityId":4, "fieldId":74, "context":"Plan (Rx): IMS Payer ID", "udaFilterUniqueId":null, "sortIndex":12
        | , "id":291321, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Plan (Rx): IMS Payer ID", "studyId":29
        | 91 }, { "udaFilterKey":null, "entityId":5, "fieldId":78, "context":"Rx Claim: Rx Fill Number", "udaFilterUniqueId"
        | :null, "sortIndex":41, "id":291350, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Rx Fill
        |  Number", "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":79, "context":"Rx Claim: Dispense as Wr
        | itten", "udaFilterUniqueId":null, "sortIndex":34, "id":291343, "configId":2911, "udaAttributeKey":null, "entityFie
        | ldLabel":"Rx Claim: Dispense as Written", "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":81, "co
        | ntext":"Rx Claim: Rx Diagnosis Code", "udaFilterUniqueId":null, "sortIndex":38, "id":291347, "configId":2911, "uda
        | AttributeKey":null, "entityFieldLabel":"Rx Claim: Rx Diagnosis Code", "studyId":2991 }, { "udaFilterKey":null, "en
        | tityId":5, "fieldId":82, "context":"Rx Claim: Supplier ID", "udaFilterUniqueId":null, "sortIndex":45, "id":291354,
        |  "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Supplier ID", "studyId":2991 }, { "udaFilt
        | erKey":null, "entityId":2, "fieldId":411, "context":"Pharmacy: Outlet Zip Code", "udaFilterUniqueId":null, "sortIn
        | dex":9, "id":291318, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Pharmacy: Outlet Zip Code", "stu
        | dyId":2991 }, { "udaFilterKey":null, "entityId":1, "fieldId":3, "context":"Product: USC 5", "udaFilterUniqueId":nu
        | ll, "sortIndex":21, "id":291330, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Product: USC 5", "st
        | udyId":2991 }, { "udaFilterKey":null, "entityId":1, "fieldId":4, "context":"Product: Product Name", "udaFilterUniq
        | ueId":null, "sortIndex":19, "id":291328, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Product: Pro
        | duct Name", "studyId":2991 }, { "udaFilterKey":null, "entityId":1, "fieldId":5, "context":"Product: Product ID", "
        | udaFilterUniqueId":null, "sortIndex":18, "id":291327, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":
        | "Product: Product ID", "studyId":2991 }, { "udaFilterKey":null, "entityId":1, "fieldId":6, "context":"Product: Pro
        | duct NDC", "udaFilterUniqueId":null, "sortIndex":20, "id":291329, "configId":2911, "udaAttributeKey":null, "entity
        | FieldLabel":"Product: Product NDC", "studyId":2991 }, { "udaFilterKey":null, "entityId":1, "fieldId":11, "context"
        | :"Product: CMF 7", "udaFilterUniqueId":null, "sortIndex":17, "id":291326, "configId":2911, "udaAttributeKey":null,
        |  "entityFieldLabel":"Product: CMF 7", "studyId":2991 }, { "udaFilterKey":null, "entityId":2, "fieldId":17, "contex
        | t":"Pharmacy: Pharmacy ID", "udaFilterUniqueId":null, "sortIndex":10, "id":291319, "configId":2911, "udaAttributeK
        | ey":null, "entityFieldLabel":"Pharmacy: Pharmacy ID", "studyId":2991 }, { "udaFilterKey":null, "entityId":3, "fiel
        | dId":33, "context":"Provider (Rx): Provider ID", "udaFilterUniqueId":null, "sortIndex":23, "id":291332, "configId"
        | :2911, "udaAttributeKey":null, "entityFieldLabel":"Provider (Rx): Provider ID", "studyId":2991 }, { "udaFilterKey"
        | :null, "entityId":3, "fieldId":37, "context":"Provider (Rx): Provider State", "udaFilterUniqueId":null, "sortIndex
        | ":24, "id":291333, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Provider (Rx): Provider State", "s
        | tudyId":2991 }, { "udaFilterKey":null, "entityId":3, "fieldId":39, "context":"Provider (Rx): Provider Zip Code", "
        | udaFilterUniqueId":null, "sortIndex":25, "id":291334, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":
        | "Provider (Rx): Provider Zip Code", "studyId":2991 }, { "udaFilterKey":null, "entityId":3, "fieldId":50, "context"
        | :"Provider (Rx): IMS Rxer ID", "udaFilterUniqueId":null, "sortIndex":22, "id":291331, "configId":2911, "udaAttribu
        | teKey":null, "entityFieldLabel":"Provider (Rx): IMS Rxer ID", "studyId":2991 }, { "udaFilterKey":null, "entityId":
        | 11, "fieldId":157, "context":"Patient: Patient Gender", "udaFilterUniqueId":null, "sortIndex":5, "id":291314, "con
        | figId":2911, "udaAttributeKey":null, "entityFieldLabel":"Patient: Patient Gender", "studyId":2991 }, { "udaFilterK
        | ey":null, "entityId":11, "fieldId":158, "context":"Patient: Patient Age", "udaFilterUniqueId":null, "sortIndex":3,
        |  "id":291312, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Patient: Patient Age", "studyId":2991 }
        | , { "udaFilterKey":null, "entityId":11, "fieldId":159, "context":"Patient: Patient ID", "udaFilterUniqueId":null,
        | "sortIndex":6, "id":291315, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Patient: Patient ID", "st
        | udyId":2991 }, { "udaFilterKey":null, "entityId":11, "fieldId":161, "context":"Patient: Patient Zip3", "udaFilterU
        | niqueId":null, "sortIndex":7, "id":291316, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Patient: P
        | atient Zip3", "studyId":2991 }, { "udaFilterKey":null, "entityId":2, "fieldId":15, "context":"Pharmacy: Channel",
        | "udaFilterUniqueId":null, "sortIndex":8, "id":291317, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":
        | "Pharmacy: Channel", "studyId":2991 }, { "udaFilterKey":null, "entityId":2, "fieldId":18, "context":"Pharmacy: Pha
        | rmacy Zip Code", "udaFilterUniqueId":null, "sortIndex":11, "id":291320, "configId":2911, "udaAttributeKey":null, "
        | entityFieldLabel":"Pharmacy: Pharmacy Zip Code", "studyId":2991 }, { "udaFilterKey":null, "entityId":3, "fieldId":
        | 32, "context":"Provider (Rx): Specialty", "udaFilterUniqueId":null, "sortIndex":26, "id":291335, "configId":2911,
        | "udaAttributeKey":null, "entityFieldLabel":"Provider (Rx): Specialty", "studyId":2991 }, { "udaFilterKey":null, "e
        | ntityId":4, "fieldId":62, "context":"Plan (Rx): Pay Type", "udaFilterUniqueId":null, "sortIndex":14, "id":291323,
        | "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Plan (Rx): Pay Type", "studyId":2991 }, { "udaFilterK
        | ey":null, "entityId":5, "fieldId":77, "context":"Rx Claim: Rx Type Code", "udaFilterUniqueId":null, "sortIndex":42
        | , "id":291351, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Rx Type Code", "studyId":299
        | 1 }, { "udaFilterKey":null, "entityId":4, "fieldId":273, "context":"Plan (Rx): IMS Plan ID", "udaFilterUniqueId":n
        | ull, "sortIndex":13, "id":291322, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Plan (Rx): IMS Plan
        |  ID", "studyId":2991 }, { "udaFilterKey":null, "entityId":1, "fieldId":171, "context":"Product: CMF 3", "udaFilter
        | UniqueId":null, "sortIndex":16, "id":291325, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Product:
        |  CMF 3", "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":477, "context":"Rx Claim: Rx Diagnosis C
        | ode Version ID", "udaFilterUniqueId":null, "sortIndex":39, "id":291348, "configId":2911, "udaAttributeKey":null, "
        | entityFieldLabel":"Rx Claim: Rx Diagnosis Code Version ID", "studyId":2991 }, { "udaFilterKey":null, "entityId":5,
        |  "fieldId":280, "context":"Rx Claim: Authorized Refill Number", "udaFilterUniqueId":null, "sortIndex":27, "id":291
        | 336, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Authorized Refill Number", "studyId":2
        | 991 }, { "udaFilterKey":null, "entityId":5, "fieldId":282, "context":"Rx Claim: BIN", "udaFilterUniqueId":null, "s
        | ortIndex":28, "id":291337, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: BIN", "studyId":
        | 2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":284, "context":"Rx Claim: Claim ID", "udaFilterUniqueId":nu
        | ll, "sortIndex":29, "id":291338, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Claim ID",
        |  "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":286, "context":"Rx Claim: Copay Amount", "udaFil
        | terUniqueId":null, "sortIndex":30, "id":291339, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Cl
        | aim: Copay Amount", "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":287, "context":"Rx Claim: Cus
        | tomer Price", "udaFilterUniqueId":null, "sortIndex":31, "id":291340, "configId":2911, "udaAttributeKey":null, "ent
        | ityFieldLabel":"Rx Claim: Customer Price", "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":288, "
        | context":"Rx Claim: DACON", "udaFilterUniqueId":null, "sortIndex":32, "id":291341, "configId":2911, "udaAttributeK
        | ey":null, "entityFieldLabel":"Rx Claim: DACON", "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":2
        | 91, "context":"Rx Claim: Days Supply", "udaFilterUniqueId":null, "sortIndex":33, "id":291342, "configId":2911, "ud
        | aAttributeKey":null, "entityFieldLabel":"Rx Claim: Days Supply", "studyId":2991 }, { "udaFilterKey":null, "entityI
        | d":5, "fieldId":292, "context":"Rx Claim: Dispensed Quantity", "udaFilterUniqueId":null, "sortIndex":35, "id":2913
        | 44, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Dispensed Quantity", "studyId":2991 },
        | { "udaFilterKey":null, "entityId":5, "fieldId":295, "context":"Rx Claim: Month ID", "udaFilterUniqueId":null, "sor
        | tIndex":36, "id":291345, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Month ID", "studyI
        | d":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":296, "context":"Rx Claim: Patient Pay Amount", "udaFilte
        | rUniqueId":null, "sortIndex":37, "id":291346, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Clai
        | m: Patient Pay Amount", "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":298, "context":"Rx Claim:
        |  Rx Dosage Amount", "udaFilterUniqueId":null, "sortIndex":40, "id":291349, "configId":2911, "udaAttributeKey":null
        | , "entityFieldLabel":"Rx Claim: Rx Dosage Amount", "studyId":2991 }, { "udaFilterKey":null, "entityId":5, "fieldId
        | ":300, "context":"Rx Claim: Rx Written Date", "udaFilterUniqueId":null, "sortIndex":43, "id":291352, "configId":29
        | 11, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Rx Written Date", "studyId":2991 }, { "udaFilterKey":nul
        | l, "entityId":5, "fieldId":302, "context":"Rx Claim: Service Date", "udaFilterUniqueId":null, "sortIndex":44, "id"
        | :291353, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Service Date", "studyId":2991 }, {
        |  "udaFilterKey":null, "entityId":5, "fieldId":305, "context":"Rx Claim: Week ID", "udaFilterUniqueId":null, "sortI
        | ndex":47, "id":291356, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: Week ID", "studyId":
        | 2991 }, { "udaFilterKey":null, "entityId":5, "fieldId":304, "context":"Rx Claim: Total Amount Paid", "udaFilterUni
        | queId":null, "sortIndex":46, "id":291355, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Rx Claim: T
        | otal Amount Paid", "studyId":2991 }, { "udaFilterKey":null, "entityId":13, "fieldId":416, "context":"Basic Extract
        | : Cohort Description", "udaFilterUniqueId":null, "sortIndex":1, "id":291310, "configId":2911, "udaAttributeKey":nu
        | ll, "entityFieldLabel":"Basic Extract: Cohort Description", "studyId":2991 }, { "udaFilterKey":null, "entityId":13
        | , "fieldId":417, "context":"Basic Extract: Cohort ID", "udaFilterUniqueId":null, "sortIndex":2, "id":291311, "conf
        | igId":2911, "udaAttributeKey":null, "entityFieldLabel":"Basic Extract: Cohort ID", "studyId":2991 }, { "udaFilterK
        | ey":null, "entityId":18, "fieldId":440, "context":"Stability/Eligibility: Patient Eligibility Indicator", "udaFilt
        | erUniqueId":null, "sortIndex":48, "id":291357, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Stabil
        | ity/Eligibility: Patient Eligibility Indicator", "studyId":2991 }, { "udaFilterKey":null, "entityId":18, "fieldId"
        | :441, "context":"Stability/Eligibility: Stable Pharmacy Indicator", "udaFilterUniqueId":null, "sortIndex":49, "id"
        | :291358, "configId":2911, "udaAttributeKey":null, "entityFieldLabel":"Stability/Eligibility: Stable Pharmacy Indic
        | ator", "studyId":2991 }, { "udaFilterKey":null, "entityId":11, "fieldId":398, "context":"Patient: Patient Birth Ye
        | ar", "udaFilterUniqueId":null, "sortIndex":4, "id":291313, "configId":2911, "udaAttributeKey":null, "entityFieldLa
        | bel":"Patient: Patient Birth Year", "studyId":2991 } ] }, "dataSourceType":"RX", "providerAttributeContext":"PROVI
        | DER", "filterCondition":"13 AND 29", "studyType":101, "timePeriod":{ "endDate":1490918400000, "lookbackPeriod":0,
        | "indexEvent":"first", "lookforwardPeriod":0, "key":2, "id":2918, "periodGrouping":"monthly", "startDate":148322880
        | 0000 }, "patientEligibility":{ "applyFlag":false, "startPointCheckLengthOfTimeMonths":null, "applyStartPointCheckF
        | lag":false, "dirtyFlag":false, "key":9, "applyEndPointCheckMonths":null, "classifyPatientsWithMarketRxFromUnstable
        | PharmaciesFlag":false, "id":2928, "applyEndPointCheckFlag":null, "applyAutoEligibilityFlag":false, "retainClaimsIn
        | eligibleFlag":true, "startPointCheckLengthOfTime":"most-recent-to", "studyId":2991 }, "filterTypes":[ { "columnGro
        | upName":"", "columnName":"mkt_id", "context":"", "patientHubTableColumnName":"product_id", "columnType":"bigint",
        | "key":13, "externalColumnName":"product_id", "tableName":"product_market_20_13_2991_7312", "filePath":"", "columnG
        | roupType":"", "externalTableName":"prod_df2_pps_batch.v_prod_mkt", "providerRelatedUdaColumn":false }, { "columnGr
        | oupName":"", "columnName":"chnl_cd", "context":"", "patientHubTableColumnName":"chnl_cd", "columnType":"string", "
        | key":29, "externalColumnName":"", "tableName":"pharmacy_channel_20_29_2991_7312", "filePath":"", "columnGroupType"
        | :"", "externalTableName":"", "providerRelatedUdaColumn":false } ], "professionalClaims":null, "sobStudy":{ "sobCon
        | tinueLevels":null, "lookbackPeriod":365, "addOn":{ "partialDaysSupplyOverlap":{ "allowableGapDays":30, "overlapOfD
        | ays":5 }, "completeDaysOverlap":true }, "lookBackTieBreaker":null, "lookForwardPeriod":120, "sobBrandLevels":null,
        |  "sobGroupInfo":null, "sobStudy":false, "sobMarketType":null, "gracePeriod":null, "sobAttrRank":null }, "pncStudy"
        | :{ "pncStudy":true, "pncGroupInfo":{ "productGroupName":"mkted_prod_nm", "classGroupName": null }, "pncMarketType"
        | :"MONO", "gracePeriod":{ "gracePeriodEnum":"NUM_DAYS", "gracePeriodValue":30 }, "lookBackTieBreaker":"HIGHEST_DAYS
        | _SUPPLY_MIN_CLAIM_ID", "pncAttrRank":{ "rank":null, "groupName":null, "fileName":null }, "cohortGroup":{ "cohortPe
        | riodGrouping":"ROLLING_QUARTERS", "startDate":1432944000000, "numOfCohortPeriods":4 }, "pncReportingIntervals":{ "
        | reportingIntervalDays":30, "reportingIntervalsToTrack":6 }, "pncReportingLevels":{ "productLevel": true, "classLev
        | el": false, "marketLevel": false }, "dataAvailability": "completeTimePeriods", "timePeriods":{ "pncLookBack":30, "
        | pncLookForward":30 }, "persistenceConf":{ "patientTherapyCategories":"PERSISTENT_RESTART_SWITCH_OFFTHERAPY_DISCONT
        | INUE", "patientTherapyStatusRule":"SWITCH_BASED_HIERARCHY", "patientTherapyStatusType":"DYNAMIC_STATUS", "persiste
        | ncePercentage":50 } }, "dataCleansing":{ "columnGroupName":null, "columnName":null, "removeExclusionListPatientsFl
        | ag":false, "daysSupplyCleansingExceedDays":365, "id":2928, "dedupAtPayTypeGroupLevelFlag":false, "removePatientsWi
        | thInvalidAgeGenderFlag":false, "dedupAtPayTypeGroupLevel":null, "rxClaimDeduppingFlag":false, "studyId":2991, "day
        | sSupplyCleansingFlag":false }, "complexQueries":{ "topicCount":null, "topic":null, "applyComplexQueriesFlag":false
        |  }, "institutionalClaims":null, "outputFields":"", "pharmacyStability":{ "minimumPercentOfWeeks":100, "applyFlag":
        | false, "dirtyFlag":false, "key":7, "id":2392, "retainClaimsUnstableFlag":true, "studyId":2991 } }, "job_informatio
        | n":{ "notification_email":"nputtabasappa@in.imshealth.com", "submitted_timestamp":"2018-06-18T16:49:51.371-04:00",
        |  "username":"20" } }
      """.stripMargin

    val done_ts = "2018-06-19T15:48:09.265Z"
    val fail_msg =
      """
        | JobException: cannot resolve '`ext.mkted_prod_nm`' given input c
        | olumns: [month_id, ims_pln_id, rx_written_dt, patient_eligibility, otlt_zip, cohort_desc, pharmacy_zip, cmf_pack_n
        | br, days_supply_cnt, diag_cd, stable_pharmacy, week_id, pat_age_nbr, pat_zip3_cd, daw_cd, dacon_qty, ims_payer_id,
        |  chnl_cd, cohort_id, diag_vers_typ_id, bin, pharmacy_id, patient_id, auth_rfll_nbr, dspnsd_qty, supplier_id, ndc,
        | svc_dt, plan_id, pat_pay_amt, total_paid_amt, rx_dosage_amt, pay_typ_cd, cust_prc, fill_nbr, pat_gender_cd, rx_typ
        | _cd, product_id, claim_id, pat_brth_yr_nbr, copay_amt]; line 11 pos 6;\x0A'Sort ['patientId ASC NULLS FIRST, 'svcD
        | t ASC NULLS FIRST, 'daysSupplyCnt ASC NULLS FIRST, 'claimId ASC NULLS FIRST], false\x0A+- 'RepartitionByExpression
        |  ['patientId], 200\x0A   +- 'Project ['patient_id AS patientId#182, 'ext.claim_id AS claimId#183, ('unix_timestamp
        | ('ext.svc_dt) * 1000) AS svcDt#184, 'ext.days_supply_cnt AS daysSupplyCnt#185, cast('ext.mkted_prod_nm as string)
        | AS pncProductGrp#186,  AS pncClassGrp#187,  AS pncMarketGrp#188]\x0A      +- 'Filter isnotnull('ext.mkted_prod_nm)
        | \x0A         +- SubqueryAlias ext\x0A            +- SubqueryAlias rx_basic_filters_2018061913501847215\x0A
        |        +- Relation[patient_eligibility#189,rx_dosage_amt#190,pharmacy_zip#191,dacon_qty#192,pay_typ_cd#193,supplie
        | r_id#194L,pat_gender_cd#195,stable_pharmacy#196,total_paid_amt#197,bin#198,chnl_cd#199,pat_zip3_cd#200,cohort_desc
        | #201,rx_typ_cd#202,pharmacy_id#203,otlt_zip#204,pat_brth_yr_nbr#205,pat_age_nbr#206L,patient_id#207L,plan_id#208,p
        | roduct_id#209,rx_written_dt#210,dspnsd_qty#211,copay_amt#212,... 17 more fields] parquet\x0A
      """.stripMargin

    val fail_trace =
      """
        | com.rxcorp.ftpa.job.JobException: cannot resolve '`ext.mkted_p
        | rod_nm`' given input columns: [month_id, ims_pln_id, rx_written_dt, patient_eligibility, otlt_zip, cohort_desc, ph
        | armacy_zip, cmf_pack_nbr, days_supply_cnt, diag_cd, stable_pharmacy, week_id, pat_age_nbr, pat_zip3_cd, daw_cd, da
        | con_qty, ims_payer_id, chnl_cd, cohort_id, diag_vers_typ_id, bin, pharmacy_id, patient_id, auth_rfll_nbr, dspnsd_q
        | ty, supplier_id, ndc, svc_dt, plan_id, pat_pay_amt, total_paid_amt, rx_dosage_amt, pay_typ_cd, cust_prc, fill_nbr,
        |  pat_gender_cd, rx_typ_cd, product_id, claim_id, pat_brth_yr_nbr, copay_amt]; line 11 pos 6;\x0A'Sort ['patientId
        | ASC NULLS FIRST, 'svcDt ASC NULLS FIRST, 'daysSupplyCnt ASC NULLS FIRST, 'claimId ASC NULLS FIRST], false\x0A+- 'R
        | epartitionByExpression ['patientId], 200\x0A   +- 'Project ['patient_id AS patientId#182, 'ext.claim_id AS claimId
        | #183, ('unix_timestamp('ext.svc_dt) * 1000) AS svcDt#184, 'ext.days_supply_cnt AS daysSupplyCnt#185, cast('ext.mkt
        | ed_prod_nm as string) AS pncProductGrp#186,  AS pncClassGrp#187,  AS pncMarketGrp#188]\x0A      +- 'Filter isnotnu
        | ll('ext.mkted_prod_nm)\x0A         +- SubqueryAlias ext\x0A            +- SubqueryAlias rx_basic_filters_201806191
        | 3501847215\x0A               +- Relation[patient_eligibility#189,rx_dosage_amt#190,pharmacy_zip#191,dacon_qty#192,
        | pay_typ_cd#193,supplier_id#194L,pat_gender_cd#195,stable_pharmacy#196,total_paid_amt#197,bin#198,chnl_cd#199,pat_z
        | ip3_cd#200,cohort_desc#201,rx_typ_cd#202,pharmacy_id#203,otlt_zip#204,pat_brth_yr_nbr#205,pat_age_nbr#206L,patient
        | _id#207L,plan_id#208,product_id#209,rx_written_dt#210,dspnsd_qty#211,copay_amt#212,... 17 more fields] parquet\x0A
        | \x0A\x09at com.rxcorp.ftpa.wfengine.spark.JobExceptionHandler$.apply(JobExceptionHandler.scala:42)\x0A\x09at com.r
        | xcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$$anonfun$run$1.apply(PnCDriver.scala:157)\x0A\x09at c
        | om.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$$anonfun$run$1.apply(PnCDriver.scala:104)\x0A\x09
        | at com.rxcorp.ftpa.wfengine.spark.SparkSessionScope$.apply(SparkSessionScope.scala:24)\x0A\x09at com.rxcorp.ftpa.w
        | fengine.spark.persistencyandcompliance.PnCDriver$.run(PnCDriver.scala:104)\x0A\x09at com.rxcorp.ftpa.wfengine.spar
        | k.persistencyandcompliance.PnCDriver$.main(PnCDriver.scala:73)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.persistenc
        | yandcompliance.PnCDriver.main(PnCDriver.scala)\x0A\x09at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Metho
        | d)\x0A\x09at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\x0A\x09at sun.reflect.D
        | elegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\x0A\x09at java.lang.reflect.Method.invoke
        | (Method.java:498)\x0A\x09at org.apache.spark.deploy.yarn.ApplicationMaster$$anon$3.run(ApplicationMaster.scala:686
        | )\x0ACaused by: org.apache.spark.sql.AnalysisException: cannot resolve '`ext.mkted_prod_nm`' given input columns:
        | [month_id, ims_pln_id, rx_written_dt, patient_eligibility, otlt_zip, cohort_desc, pharmacy_zip, cmf_pack_nbr, days
        | _supply_cnt, diag_cd, stable_pharmacy, week_id, pat_age_nbr, pat_zip3_cd, daw_cd, dacon_qty, ims_payer_id, chnl_cd
        | , cohort_id, diag_vers_typ_id, bin, pharmacy_id, patient_id, auth_rfll_nbr, dspnsd_qty, supplier_id, ndc, svc_dt,
        | plan_id, pat_pay_amt, total_paid_amt, rx_dosage_amt, pay_typ_cd, cust_prc, fill_nbr, pat_gender_cd, rx_typ_cd, pro
        | duct_id, claim_id, pat_brth_yr_nbr, copay_amt]; line 11 pos 6;\x0A'Sort ['patientId ASC NULLS FIRST, 'svcDt ASC NU
        | LLS FIRST, 'daysSupplyCnt ASC NULLS FIRST, 'claimId ASC NULLS FIRST], false\x0A+- 'RepartitionByExpression ['patie
        | ntId], 200\x0A   +- 'Project ['patient_id AS patientId#182, 'ext.claim_id AS claimId#183, ('unix_timestamp('ext.sv
        | c_dt) * 1000) AS svcDt#184, 'ext.days_supply_cnt AS daysSupplyCnt#185, cast('ext.mkted_prod_nm as string) AS pncPr
        | oductGrp#186,  AS pncClassGrp#187,  AS pncMarketGrp#188]\x0A      +- 'Filter isnotnull('ext.mkted_prod_nm)\x0A
        |      +- SubqueryAlias ext\x0A            +- SubqueryAlias rx_basic_filters_2018061913501847215\x0A               +
        | - Relation[patient_eligibility#189,rx_dosage_amt#190,pharmacy_zip#191,dacon_qty#192,pay_typ_cd#193,supplier_id#194
        | L,pat_gender_cd#195,stable_pharmacy#196,total_paid_amt#197,bin#198,chnl_cd#199,pat_zip3_cd#200,cohort_desc#201,rx_
        | typ_cd#202,pharmacy_id#203,otlt_zip#204,pat_brth_yr_nbr#205,pat_age_nbr#206L,patient_id#207L,plan_id#208,product_i
        | d#209,rx_written_dt#210,dspnsd_qty#211,copay_amt#212,... 17 more fields] parquet\x0A\x0A\x09at org.apache.spark.sq
        | l.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)\x0A\x09at org.apache.spark.sql.catalyst
        | .analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:88)\x0A\x09at or
        | g.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnal
        | ysis.scala:85)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:
        | 289)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)\x0A\x
        | 09at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)\x0A\x09at org.apache.spark.s
        | ql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:288)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$
        | $anonfun$3.apply(TreeNode.scala:286)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply(TreeN
        | ode.scala:286)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$4.apply(TreeNode.scala:306)\x0A\x09
        | at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)\x0A\x09at org.apache.spark.
        | sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:304)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode
        | .transformUp(TreeNode.scala:286)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpress
        | ionsUp$1.apply(QueryPlan.scala:268)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformExpr
        | essionsUp$1.apply(QueryPlan.scala:268)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpression
        | $1(QueryPlan.scala:279)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalyst$plan
        | s$QueryPlan$$recursiveTransform$1(QueryPlan.scala:289)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan$$an
        | onfun$6.apply(QueryPlan.scala:298)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeN
        | ode.scala:187)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:298)\x0A\x09
        | at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:268)\x0A\x09at org.apache.
        | spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:85)\x0A\x09at org.apa
        | che.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:78)\x0A\x09at org
        | .apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:127)\x0A\x09at org.apache.spark.sql.catalyst.tr
        | ees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode
        | $$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at scala.collection.immutable.List.foreach(List.scala:381)\
        | x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:126)\x0A\x09at org.apache.spark.sq
        | l.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at org.apache.spark.sql.catalyst.t
        | rees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at scala.collection.immutable.List.foreach(Lis
        | t.scala:381)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:126)\x0A\x09at org.ap
        | ache.spark.sql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at org.apache.spark.s
        | ql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at scala.collection.immutable.Lis
        | t.foreach(List.scala:381)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:126)\x0A
        | \x09at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:78)\x0A\x09at
        | org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:91)\x0A\x09at org.apache.spark.sql.ex
        | ecution.QueryExecution.assertAnalyzed(QueryExecution.scala:52)\x0A\x09at org.apache.spark.sql.Dataset$.ofRows(Data
        | set.scala:66)\x0A\x09at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:623)\x0A\x09at com.rxcorp.ftpa.wf
        | engine.spark.persistencyandcompliance.tables.PatientHub$$anonfun$1.apply(PatientHub.scala:38)\x0A\x09at com.rxcorp
        | .ftpa.wfengine.spark.persistencyandcompliance.tables.PatientHub$$anonfun$1.apply(PatientHub.scala:12)\x0A\x09at co
        | m.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.execute.ExecutePNC.invokePnC(ExecutePNC.scala:68)\x0A\x09at
        | com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.transform.PnCTransformer.transform(PnCTransformer.scala:13
        | )\x0A\x09at com.rxcorp.ftpa.wfengine.spark.job.Runner$$anonfun$run$1$$anonfun$com$rxcorp$ftpa$wfengine$spark$job$R
        | unner$$anonfun$$processTasks$1$1.apply(Runner.scala:26)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.job.Runner$$anonf
        | un$run$1$$anonfun$com$rxcorp$ftpa$wfengine$spark$job$Runner$$anonfun$$processTasks$1$1.apply(Runner.scala:23)\x0A\
        | x09at scala.collection.immutable.List.foreach(List.scala:381)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.job.Runner$
        | $anonfun$run$1.com$rxcorp$ftpa$wfengine$spark$job$Runner$$anonfun$$processTasks$1(Runner.scala:23)\x0A\x09at com.r
        | xcorp.ftpa.wfengine.spark.job.Runner$$anonfun$run$1.apply(Runner.scala:49)\x0A\x09at com.rxcorp.ftpa.wfengine.spar
        | k.job.Runner$$anonfun$run$1.apply(Runner.scala:19)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.SparkSessionScope$.app
        | ly(SparkSessionScope.scala:24)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.job.Runner.run(Runner.scala:19)\x0A\x09at
        | com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$$anonfun$run$1$$anonfun$apply$1.apply$mcV$sp(PnC
        | Driver.scala:153)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$$anonfun$run$1$$anon
        | fun$apply$1.apply(PnCDriver.scala:123)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver
        | $$anonfun$run$1$$anonfun$apply$1.apply(PnCDriver.scala:123)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.JobExceptionH
        | andler$.apply(JobExceptionHandler.scala:22)\x0A\x09... 11 more\x0A
      """.stripMargin

    val st = "failed"
    /*
    val steps =
      """
        |  [{"step_type":"persis_comp","step_id":"8","prereq_step_ids":"18,1",
        | "config":"{\x5C"pncStudy\x5C": true, \x5C"pncGroupInfo\x5C":
        | {\x5C"productGroupName\x5C": \x5C"mkted_prod_nm\x5C", \x5C"classGroupName\x5C": null},
        | \x5C"pncMarketType\x5C": \x5C"MONO\x5C", \x5C"pncReportingLevels\x5C":
        | {\x5C"productLevel\x5C": true, \x5C"classLevel\x5C": false, \x5C"marketLevel\x5C": false},
        | \x5C"gracePeriod\x5C": {\x5C"gracePeriodEnum\x5C": \x5C"NUM_DAYS\x5C",
        | \x5C"gracePeriodValue\x5C": 30}, \x5C"lookBackTieBreaker\x5C":
        | \x5C"HIGHEST_DAYS_SUPPLY_MIN_CLAIM_ID\x5C", \x5C"pncAttrRank\x5C":
        | {\x5C"fileName\x5C": null, \x5C"groupName\x5C": null, \x5C"rank\x5C": null},
        | \x5C"cohortGroup\x5C": null, \x5C"pncReportingIntervals\x5C":
        | {\x5C"reportingIntervalDays\x5C":30, \x5C"reportingIntervalsToTrack\x5C": 6},
        | \x5C"dataAvailability\x5C": \x5C"CompleteTimePeriods\x5C", \x5C"timePeriods\x5C":
        | {\x5C"pncLookBack\x5C": 30, \x5C"pncLookForward\x5C": 30}, \x5C"persistenceConf\x5C": {\x5C"patientTherapyCategories\x5C": \x5C"PERSISTENT_RESTART_SWITCH_OFFTHERAPY_DISCONTINUE\x5C",
        | \x5C"patientTherapyStatusRule\x5C": \x5C"SWITCH_BASED_HIERARCHY\x5C",
        | \x5C"patientTherapyStatusType\x5C": \x5C"DYNAMIC_STATUS\x5C",\x5C"persistencePercentage\x5C":50}}"},
        | {"step_type":"p360data","step_id":"14","prereq_step_ids":"18,1,8",
        | "config":"[{\x5C"columnName\x5C": \x5C"plan_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"plan_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"plan_id\x5C",
        | \x5C"index\x5C": 15},{\x5C"columnName\x5C": \x5C"ims_payer_id\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"ims_payer_id\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"ims_payer_id\x5C", \x5C"index\x5C": 12}, {\x5C"columnName\x5C":
        | \x5C"fill_nbr\x5C", \x5C"patientHubColumnName\x5C": \x5C"fill_nbr\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C":\x5C"fill_nbr\x5C", \x5C"index\x5C": 41}, {\x5C"columnName\x5C":
        | \x5C"daw_cd\x5C", \x5C"patientHubColumnName\x5C": \x5C"daw_cd\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"daw_cd\x5C", \x5C"index\x5C": 34}, {\x5C"columnName\x5C":
        | \x5C"diag_cd\x5C", \x5C"patientHubColumnName\x5C": \x5C"diag_cd,diag_vers_typ_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"diag_cd\x5C", \x5C"index\x5C": 38}, {\x5C"columnName\x5C":
        | \x5C"supplier_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"supplier_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"supplier_id\x5C", \x5C"index\x5C": 45},
        | {\x5C"columnName\x5C": \x5C"otlt_zip\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"otlt_zip\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"otlt_zip\x5C", \x5C"index\x5C": 9}, {\x5C"columnName\x5C": \x5C"usc_cd\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"product_id\x5C", \x5C"externalColumnName\x5C":
        | \x5C"product_id\x5C", \x5C"externalTableName\x5C": \x5C"v_product\x5C",
        | \x5C"columnAlias\x5C": \x5C"usc_cd\x5C", \x5C"index\x5C": 21}, {\x5C"columnName\x5C":
        | \x5C"mkted_prod_nm\x5C", \x5C"patientHubColumnName\x5C": \x5C"product_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"product_id\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_product\x5C", \x5C"columnAlias\x5C": \x5C"mkted_prod_nm\x5C", \x5C"index\x5C": 19},
        | {\x5C"columnName\x5C": \x5C"product_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"product_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"product_id\x5C", \x5C"index\x5C": 18}, {\x5C"columnName\x5C":
        | \x5C"ndc\x5C", \x5C"patientHubColumnName\x5C": \x5C"ndc\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"ndc\x5C",
        | \x5C"index\x5C": 20}, {\x5C"columnName\x5C": \x5C"cmf_prod_nbr\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"product_id\x5C", \x5C"externalColumnName\x5C":
        | \x5C"product_id\x5C", \x5C"externalTableName\x5C": \x5C"v_product\x5C",
        | \x5C"columnAlias\x5C": \x5C"cmf_prod_nbr\x5C", \x5C"index\x5C": 17},
        | {\x5C"columnName\x5C": \x5C"pharmacy_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pharmacy_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"pharmacy_id\x5C",
        | \x5C"index\x5C": 10}, {\x5C"columnName\x5C": \x5C"provider_id\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"v_provider\x5C", \x5C"columnAlias\x5C": \x5C"provider_id\x5C",
        | \x5C"index\x5C": 23}, {\x5C"columnName\x5C": \x5C"st_cd\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_zip\x5C", \x5C"columnAlias\x5C": \x5C"provider_provider_st_cd\x5C",
        | \x5C"index\x5C": 24}, {\x5C"columnName\x5C": \x5C"zip\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_zip\x5C", \x5C"columnAlias\x5C": \x5C"provider_zip\x5C", \x5C"index\x5C": 25},
        | {\x5C"columnName\x5C": \x5C"ims_rxer_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_provider\x5C", \x5C"columnAlias\x5C": \x5C"rxer_id\x5C", \x5C"index\x5C": 22},
        | {\x5C"columnName\x5C": \x5C"pat_gender_cd\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pat_gender_cd\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"pat_gender_cd\x5C", \x5C"index\x5C": 5},
        | {\x5C"columnName\x5C": \x5C"pat_age_nbr\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pat_age_nbr\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"pat_age_nbr\x5C", \x5C"index\x5C": 3},
        | {\x5C"columnName\x5C": \x5C"patient_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"patient_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"patient_id\x5C", \x5C"index\x5C": 6},
        | {\x5C"columnName\x5C": \x5C"pat_zip3_cd\x5C", \x5C"patientHubColumnName\x5C":
        |\ \x5C"pat_zip3_cd\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"pat_zip3_cd\x5C",
        | \x5C"index\x5C": 7}, {\x5C"columnName\x5C": \x5C"chnl_cd\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"chnl_cd\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"chnl_cd\x5C", \x5C"index\x5C": 8},
        | {\x5C"columnName\x5C": \x5C"pharmacy_zip\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pharmacy_zip\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"pharmacy_zip\x5C", \x5C"index\x5C": 11}, {\x5C"columnName\x5C":
        | \x5C"pri_spcl_cd\x5C", \x5C"patientHubColumnName\x5C": \x5C"\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_provider\x5C", \x5C"columnAlias\x5C": \x5C"provider_pri_spcl_cd\x5C",
        | \x5C"index\x5C": 26}, {\x5C"columnName\x5C": \x5C"pay_typ_cd\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"pay_typ_cd\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"pay_typ_cd\x5C", \x5C"index\x5C": 14}, {\x5C"columnName\x5C":
        | \x5C"rx_typ_cd\x5C", \x5C"patientHubColumnName\x5C": \x5C"rx_typ_cd\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"rx_typ_cd\x5C", \x5C"index\x5C": 42}, {\x5C"columnName\x5C":
        | \x5C"ims_pln_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"ims_pln_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"ims_pln_id\x5C", \x5C"index\x5C": 13}, {\x5C"columnName\x5C":
        | \x5C"cmf_pack_nbr\x5C", \x5C"patientHubColumnName\x5C": \x5C"cmf_pack_nbr\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",\
        | \x5C"columnAlias\x5C": \x5C"cmf_pack_nbr\x5C", \x5C"index\x5C": 16}, {\x5C"columnName\x5C":
        | \x5C"diag_vers_typ_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"diag_vers_typ_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"diag_vers_typ_id\x5C", \x5C"index\x5C": 39},
        | {\x5C"columnName\x5C": \x5C"auth_rfll_nbr\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"auth_rfll_nbr\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"auth_rfll_nbr\x5C",
        | \x5C"index\x5C": 27},{\x5C"columnName\x5C": \x5C"bin\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"bin\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"bin\x5C", \x5C"index\x5C": 28},
        | {\x5C"columnName\x5C": \x5C"claim_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"claim_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"claim_id\x5C",
        | \x5C"index\x5C": 29}, {\x5C"columnName\x5C": \x5C"copay_amt\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"copay_amt\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"copay_amt\x5C", \x5C"index\x5C": 30}, {\x5C"columnName\x5C":
        | \x5C"cust_prc\x5C", \x5C"patientHubColumnName\x5C": \x5C"cust_prc\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"cust_prc\x5C", \x5C"index\x5C": 31},
        | {\x5C"columnName\x5C": \x5C"dacon_qty\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"dacon_qty\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C",\x5C"columnAlias\x5C": \x5C"dacon_qty\x5C", \x5C"index\x5C": 32},
        | {\x5C"columnName\x5C": \x5C"days_supply_cnt\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"days_supply_cnt\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"days_supply_cnt\x5C",
        | \x5C"index\x5C": 33}, {\x5C"columnName\x5C": \x5C"dspnsd_qty\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"dspnsd_qty\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"dspnsd_qty\x5C", \x5C"index\x5C": 35}, {\x5C"columnName\x5C":
        | \x5C"month_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"month_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"month_id\x5C", \x5C"index\x5C": 36},
        | {\x5C"columnName\x5C": \x5C"pat_pay_amt\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pat_pay_amt\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"pat_pay_amt\x5C", \x5C"index\x5C": 37}, {\x5C"columnName\x5C":
        | \x5C"rx_dosage_amt\x5C", \x5C"patientHubColumnName\x5C": \x5C"rx_dosage_amt\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"rx_dosage_amt\x5C", \x5C"index\x5C": 40},
        | {\x5C"columnName\x5C": \x5C"rx_written_dt\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"rx_written_dt\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"rx_written_dt\x5C",
        | \x5C"index\x5C": 43}, {\x5C"columnName\x5C": \x5C"svc_dt\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"svc_dt\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"svc_dt\x5C", \x5C"index\x5C": 44}, {\x5C"columnName\x5C": \x5C"week_id\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"week_id\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"week_id\x5C", \x5C"index\x5C": 47}, {\x5C"columnName\x5C":
        | \x5C"total_paid_amt\x5C", \x5C"patientHubColumnName\x5C": \x5C"total_paid_amt\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C":\x5C"total_paid_amt\x5C", \x5C"index\x5C": 46},
        | {\x5C"columnName\x5C": \x5C"cohort_desc\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"cohort_desc\x5C", \x5C"index\x5C": 1},
        | {\x5C"columnName\x5C": \x5C"cohort_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"cohort_id\x5C", \x5C"index\x5C": 2},
        | {\x5C"columnName\x5C": \x5C"patient_eligibility\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"patient_eligibility\x5C", \x5C"index\x5C": 48}, {\x5C"columnName\x5C":
        | \x5C"stable_pharmacy\x5C", \x5C"patientHubColumnName\x5C": \x5C"\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"stable_pharmacy\x5C", \x5C"index\x5C": 49},
        | {\x5C"columnName\x5C": \x5C"pat_brth_yr_nbr\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pat_brth_yr_nbr\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"pat_brth_yr_nbr\x5C", \x5C"index\x5C":4}]"},
        | {"step_type":"ndv_count","step_id":"18","prereq_step_ids":"",
        | "config":"[{\x5C"key\x5C": \x5C"13\x5C", \x5C"filePath\x5C": \x5C"\x5C",
        | \x5C"columnName\x5C": \x5C"mkt_id\x5C", \x5C"columnType\x5C": \x5C"bigint\x5C", \x5C"patientHubTableColumnName\x5C": \x5C"product_id\x5C",
        | \x5C"columnGroupName\x5C": \x5C"\x5C", \x5C"columnGroupType\x5C":
        | \x5C"\x5C", \x5C"tableName\x5C": \x5C"product_market_20_13_2991_7312\x5C",
        | \x5C"externalTableName\x5C": \x5C"prod_df2_pps_batch.v_prod_mkt\x5C",
        | \x5C"externalColumnName\x5C": \x5C"product_id\x5C", \x5C"operator\x5C":
        | \x5C"EQ\x5C", \x5C"whereConditions\x5C": null, \x5C"groupByParams\x5C": null,
        | \x5C"timeFilter\x5C": null}, {\x5C"key\x5C": \x5C"29\x5C", \x5C"filePath\x5C":
        | \x5C"\x5C", \x5C"columnName\x5C": \x5C"chnl_cd\x5C", \x5C"columnType\x5C":
        | \x5C"string\x5C", \x5C"patientHubTableColumnName\x5C": \x5C"chnl_cd\x5C",
        | \x5C"columnGroupName\x5C": \x5C"\x5C", \x5C"columnGroupType\x5C": \x5C"\x5C",
        | \x5C"tableName\x5C": \x5C"pharmacy_channel_20_29_2991_7312\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"operator\x5C": \x5C"EQ\x5C",
        | \x5C"whereConditions\x5C": null, \x5C"groupByParams\x5C": null, \x5C"timeFilter\x5C": null}]"},{"step_type":"basic_filters","keys":"13,29","step_id":"1","prereq_step_ids":"18",
        | "config":"[{\x5C"key\x5C": \x5C"13\x5C", \x5C"filePath\x5C": \x5C"\x5C",
        | \x5C"columnName\x5C": \x5C"mkt_id\x5C", \x5C"columnType\x5C": \x5C"bigint\x5C",
        | \x5C"patientHubTableColumnName\x5C": \x5C"product_id\x5C", \x5C"columnGroupName\x5C":
        | \x5C"\x5C", \x5C"columnGroupType\x5C": \x5C"\x5C", \x5C"tableName\x5C":
        | \x5C"product_market_20_13_2991_7312\x5C", \x5C"externalTableName\x5C":
        | \x5C"prod_df2_pps_batch.v_prod_mkt\x5C", \x5C"externalColumnName\x5C":
        | \x5C"product_id\x5C", \x5C"operator\x5C": \x5C"EQ\x5C", \x5C"whereConditions\x5C":
        | null, \x5C"groupByParams\x5C": null, \x5C"timeFilter\x5C": null}, {\x5C"key\x5C":
        | \x5C"29\x5C", \x5C"filePath\x5C": \x5C"\x5C", \x5C"columnName\x5C": \x5C"chnl_cd\x5C",
        | \x5C"columnType\x5C": \x5C"string\x5C", \x5C"patientHubTableColumnName\x5C":
        | \x5C"chnl_cd\x5C", \x5C"columnGroupName\x5C": \x5C"\x5C", \x5C"columnGroupType\x5C":
        | \x5C"\x5C", \x5C"tableName\x5C": \x5C"pharmacy_channel_20_29_2991_7312\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"operator\x5C": \x5C"EQ\x5C", \x5C"whereConditions\x5C": null,
        | \x5C"groupByParams\x5C": null, \x5C"timeFilter\x5C": null}]"}]
      """.stripMargin
    */
    /*
    val steps =
      """
        |  [{"step_type":"persis_comp","step_id":"8","prereq_step_ids":"18,1",
        | "config":"{\x5C"pncStudy\x5C": true, \x5C"pncGroupInfo\x5C":
        | {\x5C"productGroupName\x5C": \x5C"mkted_prod_nm\x5C", \x5C"classGroupName\x5C": null},
        | \x5C"pncMarketType\x5C": \x5C"MONO\x5C", \x5C"pncReportingLevels\x5C":
        | {\x5C"productLevel\x5C": true, \x5C"classLevel\x5C": false, \x5C"marketLevel\x5C": false},
        | \x5C"gracePeriod\x5C": {\x5C"gracePeriodEnum\x5C": \x5C"NUM_DAYS\x5C",
        | \x5C"gracePeriodValue\x5C": 30}, \x5C"lookBackTieBreaker\x5C":
        | \x5C"HIGHEST_DAYS_SUPPLY_MIN_CLAIM_ID\x5C", \x5C"pncAttrRank\x5C":
        | {\x5C"fileName\x5C": null, \x5C"groupName\x5C": null, \x5C"rank\x5C": null},
        | \x5C"cohortGroup\x5C": null, \x5C"pncReportingIntervals\x5C":
        | {\x5C"reportingIntervalDays\x5C":30, \x5C"reportingIntervalsToTrack\x5C": 6},
        | \x5C"dataAvailability\x5C": \x5C"CompleteTimePeriods\x5C", \x5C"timePeriods\x5C":
        | {\x5C"pncLookBack\x5C": 30, \x5C"pncLookForward\x5C": 30}, \x5C"persistenceConf\x5C": {\x5C"patientTherapyCategories\x5C": \x5C"PERSISTENT_RESTART_SWITCH_OFFTHERAPY_DISCONTINUE\x5C",
        | \x5C"patientTherapyStatusRule\x5C": \x5C"SWITCH_BASED_HIERARCHY\x5C",
        | \x5C"patientTherapyStatusType\x5C": \x5C"DYNAMIC_STATUS\x5C",\x5C"persistencePercentage\x5C":50}}"},
        | {"step_type":"p360data","step_id":"14","prereq_step_ids":"18,1,8",
        | "config":"[{\x5C"columnName\x5C": \x5C"plan_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"plan_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"plan_id\x5C",
        | \x5C"index\x5C": 15},{\x5C"columnName\x5C": \x5C"ims_payer_id\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"ims_payer_id\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"ims_payer_id\x5C", \x5C"index\x5C": 12}, {\x5C"columnName\x5C":
        | \x5C"fill_nbr\x5C", \x5C"patientHubColumnName\x5C": \x5C"fill_nbr\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C":\x5C"fill_nbr\x5C", \x5C"index\x5C": 41}, {\x5C"columnName\x5C":
        | \x5C"daw_cd\x5C", \x5C"patientHubColumnName\x5C": \x5C"daw_cd\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"daw_cd\x5C", \x5C"index\x5C": 34}, {\x5C"columnName\x5C":
        | \x5C"diag_cd\x5C", \x5C"patientHubColumnName\x5C": \x5C"diag_cd,diag_vers_typ_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"diag_cd\x5C", \x5C"index\x5C": 38}, {\x5C"columnName\x5C":
        | \x5C"supplier_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"supplier_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"supplier_id\x5C", \x5C"index\x5C": 45},
        | {\x5C"columnName\x5C": \x5C"otlt_zip\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"otlt_zip\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"otlt_zip\x5C", \x5C"index\x5C": 9}, {\x5C"columnName\x5C": \x5C"usc_cd\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"product_id\x5C", \x5C"externalColumnName\x5C":
        | \x5C"product_id\x5C", \x5C"externalTableName\x5C": \x5C"v_product\x5C",
        | \x5C"columnAlias\x5C": \x5C"usc_cd\x5C", \x5C"index\x5C": 21}, {\x5C"columnName\x5C":
        | \x5C"mkted_prod_nm\x5C", \x5C"patientHubColumnName\x5C": \x5C"product_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"product_id\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_product\x5C", \x5C"columnAlias\x5C": \x5C"mkted_prod_nm\x5C", \x5C"index\x5C": 19},
        | {\x5C"columnName\x5C": \x5C"product_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"product_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"product_id\x5C", \x5C"index\x5C": 18}, {\x5C"columnName\x5C":
        | \x5C"ndc\x5C", \x5C"patientHubColumnName\x5C": \x5C"ndc\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"ndc\x5C",
        | \x5C"index\x5C": 20}, {\x5C"columnName\x5C": \x5C"cmf_prod_nbr\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"product_id\x5C", \x5C"externalColumnName\x5C":
        | \x5C"product_id\x5C", \x5C"externalTableName\x5C": \x5C"v_product\x5C",
        | \x5C"columnAlias\x5C": \x5C"cmf_prod_nbr\x5C", \x5C"index\x5C": 17},
        | {\x5C"columnName\x5C": \x5C"pharmacy_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pharmacy_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"pharmacy_id\x5C",
        | \x5C"index\x5C": 10}, {\x5C"columnName\x5C": \x5C"provider_id\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"v_provider\x5C", \x5C"columnAlias\x5C": \x5C"provider_id\x5C",
        | \x5C"index\x5C": 23}, {\x5C"columnName\x5C": \x5C"st_cd\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_zip\x5C", \x5C"columnAlias\x5C": \x5C"provider_provider_st_cd\x5C",
        | \x5C"index\x5C": 24}, {\x5C"columnName\x5C": \x5C"zip\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_zip\x5C", \x5C"columnAlias\x5C": \x5C"provider_zip\x5C", \x5C"index\x5C": 25},
        | {\x5C"columnName\x5C": \x5C"ims_rxer_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_provider\x5C", \x5C"columnAlias\x5C": \x5C"rxer_id\x5C", \x5C"index\x5C": 22},
        | {\x5C"columnName\x5C": \x5C"pat_gender_cd\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pat_gender_cd\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"pat_gender_cd\x5C", \x5C"index\x5C": 5},
        | {\x5C"columnName\x5C": \x5C"pat_age_nbr\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pat_age_nbr\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"pat_age_nbr\x5C", \x5C"index\x5C": 3},
        | {\x5C"columnName\x5C": \x5C"patient_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"patient_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"patient_id\x5C", \x5C"index\x5C": 6},
        | {\x5C"columnName\x5C": \x5C"pat_zip3_cd\x5C", \x5C"patientHubColumnName\x5C":
        |\ \x5C"pat_zip3_cd\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"pat_zip3_cd\x5C",
        | \x5C"index\x5C": 7}, {\x5C"columnName\x5C": \x5C"chnl_cd\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"chnl_cd\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"chnl_cd\x5C", \x5C"index\x5C": 8},
        | {\x5C"columnName\x5C": \x5C"pharmacy_zip\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pharmacy_zip\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"pharmacy_zip\x5C", \x5C"index\x5C": 11}, {\x5C"columnName\x5C":
        | \x5C"pri_spcl_cd\x5C", \x5C"patientHubColumnName\x5C": \x5C"\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"v_provider\x5C", \x5C"columnAlias\x5C": \x5C"provider_pri_spcl_cd\x5C",
        | \x5C"index\x5C": 26}, {\x5C"columnName\x5C": \x5C"pay_typ_cd\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"pay_typ_cd\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"pay_typ_cd\x5C", \x5C"index\x5C": 14}, {\x5C"columnName\x5C":
        | \x5C"rx_typ_cd\x5C", \x5C"patientHubColumnName\x5C": \x5C"rx_typ_cd\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"rx_typ_cd\x5C", \x5C"index\x5C": 42}, {\x5C"columnName\x5C":
        | \x5C"ims_pln_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"ims_pln_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"ims_pln_id\x5C", \x5C"index\x5C": 13}, {\x5C"columnName\x5C":
        | \x5C"cmf_pack_nbr\x5C", \x5C"patientHubColumnName\x5C": \x5C"cmf_pack_nbr\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",\
        | \x5C"columnAlias\x5C": \x5C"cmf_pack_nbr\x5C", \x5C"index\x5C": 16}, {\x5C"columnName\x5C":
        | \x5C"diag_vers_typ_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"diag_vers_typ_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"diag_vers_typ_id\x5C", \x5C"index\x5C": 39},
        | {\x5C"columnName\x5C": \x5C"auth_rfll_nbr\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"auth_rfll_nbr\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"auth_rfll_nbr\x5C",
        | \x5C"index\x5C": 27},{\x5C"columnName\x5C": \x5C"bin\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"bin\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"bin\x5C", \x5C"index\x5C": 28},
        | {\x5C"columnName\x5C": \x5C"claim_id\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"claim_id\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"claim_id\x5C",
        | \x5C"index\x5C": 29}, {\x5C"columnName\x5C": \x5C"copay_amt\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"copay_amt\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"copay_amt\x5C", \x5C"index\x5C": 30}, {\x5C"columnName\x5C":
        | \x5C"cust_prc\x5C", \x5C"patientHubColumnName\x5C": \x5C"cust_prc\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"cust_prc\x5C", \x5C"index\x5C": 31},
        | {\x5C"columnName\x5C": \x5C"dacon_qty\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"dacon_qty\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C",\x5C"columnAlias\x5C": \x5C"dacon_qty\x5C", \x5C"index\x5C": 32},
        | {\x5C"columnName\x5C": \x5C"days_supply_cnt\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"days_supply_cnt\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"days_supply_cnt\x5C",
        | \x5C"index\x5C": 33}, {\x5C"columnName\x5C": \x5C"dspnsd_qty\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"dspnsd_qty\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"dspnsd_qty\x5C", \x5C"index\x5C": 35}, {\x5C"columnName\x5C":
        | \x5C"month_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"month_id\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"month_id\x5C", \x5C"index\x5C": 36},
        | {\x5C"columnName\x5C": \x5C"pat_pay_amt\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pat_pay_amt\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"pat_pay_amt\x5C", \x5C"index\x5C": 37}, {\x5C"columnName\x5C":
        | \x5C"rx_dosage_amt\x5C", \x5C"patientHubColumnName\x5C": \x5C"rx_dosage_amt\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"rx_dosage_amt\x5C", \x5C"index\x5C": 40},
        | {\x5C"columnName\x5C": \x5C"rx_written_dt\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"rx_written_dt\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"rx_written_dt\x5C",
        | \x5C"index\x5C": 43}, {\x5C"columnName\x5C": \x5C"svc_dt\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"svc_dt\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"svc_dt\x5C", \x5C"index\x5C": 44}, {\x5C"columnName\x5C": \x5C"week_id\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"week_id\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"week_id\x5C", \x5C"index\x5C": 47}, {\x5C"columnName\x5C":
        | \x5C"total_paid_amt\x5C", \x5C"patientHubColumnName\x5C": \x5C"total_paid_amt\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C":\x5C"total_paid_amt\x5C", \x5C"index\x5C": 46},
        | {\x5C"columnName\x5C": \x5C"cohort_desc\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"cohort_desc\x5C", \x5C"index\x5C": 1},
        | {\x5C"columnName\x5C": \x5C"cohort_id\x5C", \x5C"patientHubColumnName\x5C": \x5C"\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C",
        | \x5C"columnAlias\x5C": \x5C"cohort_id\x5C", \x5C"index\x5C": 2},
        | {\x5C"columnName\x5C": \x5C"patient_eligibility\x5C",
        | \x5C"patientHubColumnName\x5C": \x5C"\x5C", \x5C"externalColumnName\x5C":
        | \x5C"\x5C", \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"patient_eligibility\x5C", \x5C"index\x5C": 48}, {\x5C"columnName\x5C":
        | \x5C"stable_pharmacy\x5C", \x5C"patientHubColumnName\x5C": \x5C"\x5C",
        | \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"columnAlias\x5C": \x5C"stable_pharmacy\x5C", \x5C"index\x5C": 49},
        | {\x5C"columnName\x5C": \x5C"pat_brth_yr_nbr\x5C", \x5C"patientHubColumnName\x5C":
        | \x5C"pat_brth_yr_nbr\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"columnAlias\x5C":
        | \x5C"pat_brth_yr_nbr\x5C", \x5C"index\x5C":4}]"},
        | {"step_type":"ndv_count","step_id":"18","prereq_step_ids":"",
        | "config":"[{\x5C"key\x5C": \x5C"13\x5C", \x5C"filePath\x5C": \x5C"\x5C",
        | \x5C"columnName\x5C": \x5C"mkt_id\x5C", \x5C"columnType\x5C": \x5C"bigint\x5C", \x5C"patientHubTableColumnName\x5C": \x5C"product_id\x5C",
        | \x5C"columnGroupName\x5C": \x5C"\x5C", \x5C"columnGroupType\x5C":
        | \x5C"\x5C", \x5C"tableName\x5C": \x5C"product_market_20_13_2991_7312\x5C",
        | \x5C"externalTableName\x5C": \x5C"prod_df2_pps_batch.v_prod_mkt\x5C",
        | \x5C"externalColumnName\x5C": \x5C"product_id\x5C", \x5C"operator\x5C":
        | \x5C"EQ\x5C", \x5C"whereConditions\x5C": null, \x5C"groupByParams\x5C": null,
        | \x5C"timeFilter\x5C": null}, {\x5C"key\x5C": \x5C"29\x5C", \x5C"filePath\x5C":
        | \x5C"\x5C", \x5C"columnName\x5C": \x5C"chnl_cd\x5C", \x5C"columnType\x5C":
        | \x5C"string\x5C", \x5C"patientHubTableColumnName\x5C": \x5C"chnl_cd\x5C",
        | \x5C"columnGroupName\x5C": \x5C"\x5C", \x5C"columnGroupType\x5C": \x5C"\x5C",
        | \x5C"tableName\x5C": \x5C"pharmacy_channel_20_29_2991_7312\x5C", \x5C"externalTableName\x5C":
        | \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C", \x5C"operator\x5C": \x5C"EQ\x5C",
        | \x5C"whereConditions\x5C": null, \x5C"groupByParams\x5C": null, \x5C"timeFilter\x5C": null}]"},{"step_type":"basic_filters","keys":"13,29","step_id":"1","prereq_step_ids":"18",
        | "config":"[{\x5C"key\x5C": \x5C"13\x5C", \x5C"filePath\x5C": \x5C"\x5C",
        | \x5C"columnName\x5C": \x5C"mkt_id\x5C", \x5C"columnType\x5C": \x5C"bigint\x5C",
        | \x5C"patientHubTableColumnName\x5C": \x5C"product_id\x5C", \x5C"columnGroupName\x5C":
        | \x5C"\x5C", \x5C"columnGroupType\x5C": \x5C"\x5C", \x5C"tableName\x5C":
        | \x5C"product_market_20_13_2991_7312\x5C", \x5C"externalTableName\x5C":
        | \x5C"prod_df2_pps_batch.v_prod_mkt\x5C", \x5C"externalColumnName\x5C":
        | \x5C"product_id\x5C", \x5C"operator\x5C": \x5C"EQ\x5C", \x5C"whereConditions\x5C":
        | null, \x5C"groupByParams\x5C": null, \x5C"timeFilter\x5C": null}, {\x5C"key\x5C":
        | \x5C"29\x5C", \x5C"filePath\x5C": \x5C"\x5C", \x5C"columnName\x5C": \x5C"chnl_cd\x5C",
        | \x5C"columnType\x5C": \x5C"string\x5C", \x5C"patientHubTableColumnName\x5C":
        | \x5C"chnl_cd\x5C", \x5C"columnGroupName\x5C": \x5C"\x5C", \x5C"columnGroupType\x5C":
        | \x5C"\x5C", \x5C"tableName\x5C": \x5C"pharmacy_channel_20_29_2991_7312\x5C",
        | \x5C"externalTableName\x5C": \x5C"\x5C", \x5C"externalColumnName\x5C": \x5C"\x5C",
        | \x5C"operator\x5C": \x5C"EQ\x5C", \x5C"whereConditions\x5C": null,
        | \x5C"groupByParams\x5C": null, \x5C"timeFilter\x5C": null}]"}]
      """.stripMargin
    */

    val steps =
      """
        | [
        |   {
        |     "step_type":"persis_comp8","step_id":"8","prereq_step_ids":"18,1",
        |     "config":{
        |       "pncStudy": "true", "pncGroupInfo": "productGroupName", "mkted_prod_nmC" : "testName"
        |     }
        |   },
        |   {
        |     "step_type":"persis_comp14","step_id":"14","prereq_step_ids":"18,1",
        |     "config":{
        |       "pncStudy": "true", "pncGroupInfo": "productGroupName", "mkted_prod_nmC" : "testName"
        |     }
        |   },
        |   {
        |     "step_type":"persis_comp18","step_id":"18","prereq_step_ids":"18,1",
        |     "config":{
        |       "pncStudy": "true", "pncGroupInfo": "productGroupName", "mkted_prod_nmC" : "testName"
        |     }
        |   },
        |   {
        |     "step_type":"persis_comp1","step_id":"1","prereq_step_ids":"18,1",
        |     "config":{
        |       "pncStudy": "true", "pncGroupInfo": "productGroupName", "mkted_prod_nmC" : "testName"
        |     }
        |   }
        | ]
      """.stripMargin

    val stp_14_pend_ts = "2018-06-19T13:50:37.234Z"
    val stp_14_status = "pending"
    val stp_14_typ = "p360data"
    val stp_18_attr =
      """
        | {"ndv_column":"2417466"}
      """.stripMargin
    val stp_18_done_ts = "2018-06-19T13:51:33.504Z"
    val stp_18_pend_ts = "2018-06-19T13:50:37.234Z"
    val stp_18_run_ts = "2018-06-19T13:50:47.424Z"
    val stp_18_status = "completed"
    val stp_18_typ = "ndv_count"
    val stp_1_attr =
      """
        | {"outputtable_basic_filters":"prod_df2_pps_batch.rx_basic_filters_2018061913501847215","rowcount_basic_filters":"2360946"}
      """.stripMargin
    val stp_1_done_ts = "2018-06-19T13:52:00.324Z"
    val stp_1_pend_ts = "2018-06-19T13:50:37.235Z"
    val stp_1_run_ts = "2018-06-19T13:51:38.340Z"
    val stp_1_status = "completed"
    val stp_1_typ = "basic_filters"
    val stp_8_done_ts = "2018-06-19T15:48:09.256Z"
    val stp_8_fail_msg =
      """
        | JobException: cannot resolve '`ext.mkted_prod_nm`' given input columns:
        | [month_id, ims_pln_id, rx_written_dt, patient_eligibility, otlt_zip, cohort_desc, pharmacy_zip,
        | cmf_pack_nbr, days_supply_cnt, diag_cd, stable_pharmacy, week_id, pat_age_nbr, pat_zip3_cd, daw_cd,
        | dacon_qty, ims_payer_id, chnl_cd, cohort_id, diag_vers_typ_id, bin, pharmacy_id, patient_id, auth_rfll_nbr,
        | dspnsd_qty, supplier_id, ndc, svc_dt, plan_id, pat_pay_amt, total_paid_amt, rx_dosage_amt, pay_typ_cd,
        | cust_prc, fill_nbr, pat_gender_cd, rx_typ_cd, product_id, claim_id, pat_brth_yr_nbr, copay_amt];
        | line 11 pos 6;\x0A'Sort ['patientId ASC NULLS FIRST, 'svcDt ASC NULLS FIRST, 'daysSupplyCnt ASC NULLS FIRST,
        | 'claimId ASC NULLS FIRST], false\x0A+- 'RepartitionByExpression ['patientId], 200\x0A   +-
        | 'Project ['patient_id AS patientId#182, 'ext.claim_id AS claimId#183, ('unix_timestamp('ext.svc_dt) * 1000)
        | AS svcDt#184, 'ext.days_supply_cnt AS daysSupplyCnt#185, cast('ext.mkted_prod_nm as string) AS pncProductGrp#186,
        | AS pncClassGrp#187,  AS pncMarketGrp#188]\x0A      +- 'Filter isnotnull('ext.mkted_prod_nm)\x0A
        |  +- SubqueryAlias ext\x0A            +- SubqueryAlias rx_basic_filters_2018061913501847215\x0A
        |   +- Relation[patient_eligibility#189,rx_dosage_amt#190,pharmacy_zip#191,dacon_qty#192,pay_typ_cd#193,
        |   supplier_id#194L,pat_gender_cd#195,stable_pharmacy#196,total_paid_amt#197,bin#198,chnl_cd#199,
        |   pat_zip3_cd#200,cohort_desc#201,rx_typ_cd#202,pharmacy_id#203,otlt_zip#204,pat_brth_yr_nbr#205,
        |   pat_age_nbr#206L,patient_id#207L,plan_id#208,product_id#209,rx_written_dt#210,dspnsd_qty#211,
        |   copay_amt#212,... 17 more fields] parquet\x0A
      """.stripMargin
    val stp_8_fail_trace =
      """
        | com.rxcorp.ftpa.job.JobException: cannot resolve '`ext.m
        | kted_prod_nm`' given input columns: [month_id, ims_pln_id, rx_written_dt, patient_eligibility, otlt_zip, cohort_de
        | sc, pharmacy_zip, cmf_pack_nbr, days_supply_cnt, diag_cd, stable_pharmacy, week_id, pat_age_nbr, pat_zip3_cd, daw_
        | cd, dacon_qty, ims_payer_id, chnl_cd, cohort_id, diag_vers_typ_id, bin, pharmacy_id, patient_id, auth_rfll_nbr, ds
        | pnsd_qty, supplier_id, ndc, svc_dt, plan_id, pat_pay_amt, total_paid_amt, rx_dosage_amt, pay_typ_cd, cust_prc, fil
        | l_nbr, pat_gender_cd, rx_typ_cd, product_id, claim_id, pat_brth_yr_nbr, copay_amt]; line 11 pos 6;\x0A'Sort ['pati
        | entId ASC NULLS FIRST, 'svcDt ASC NULLS FIRST, 'daysSupplyCnt ASC NULLS FIRST, 'claimId ASC NULLS FIRST], false\x0
        | A+- 'RepartitionByExpression ['patientId], 200\x0A   +- 'Project ['patient_id AS patientId#182, 'ext.claim_id AS c
        | laimId#183, ('unix_timestamp('ext.svc_dt) * 1000) AS svcDt#184, 'ext.days_supply_cnt AS daysSupplyCnt#185, cast('e
        | xt.mkted_prod_nm as string) AS pncProductGrp#186,  AS pncClassGrp#187,  AS pncMarketGrp#188]\x0A      +- 'Filter i
        | snotnull('ext.mkted_prod_nm)\x0A         +- SubqueryAlias ext\x0A            +- SubqueryAlias rx_basic_filters_201
        | 8061913501847215\x0A               +- Relation[patient_eligibility#189,rx_dosage_amt#190,pharmacy_zip#191,dacon_qt
        | y#192,pay_typ_cd#193,supplier_id#194L,pat_gender_cd#195,stable_pharmacy#196,total_paid_amt#197,bin#198,chnl_cd#199
        | ,pat_zip3_cd#200,cohort_desc#201,rx_typ_cd#202,pharmacy_id#203,otlt_zip#204,pat_brth_yr_nbr#205,pat_age_nbr#206L,p
        | atient_id#207L,plan_id#208,product_id#209,rx_written_dt#210,dspnsd_qty#211,copay_amt#212,... 17 more fields] parqu
        | et\x0A\x0A\x09at com.rxcorp.ftpa.wfengine.spark.JobExceptionHandler$.apply(JobExceptionHandler.scala:42)\x0A\x09at
        |  com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$$anonfun$run$1.apply(PnCDriver.scala:157)\x0A\x
        | 09at com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$$anonfun$run$1.apply(PnCDriver.scala:104)\x
        | 0A\x09at com.rxcorp.ftpa.wfengine.spark.SparkSessionScope$.apply(SparkSessionScope.scala:24)\x0A\x09at com.rxcorp.
        | ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$.run(PnCDriver.scala:104)\x0A\x09at com.rxcorp.ftpa.wfengin
        | e.spark.persistencyandcompliance.PnCDriver$.main(PnCDriver.scala:73)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.pers
        | istencyandcompliance.PnCDriver.main(PnCDriver.scala)\x0A\x09at sun.reflect.NativeMethodAccessorImpl.invoke0(Native
        |  Method)\x0A\x09at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)\x0A\x09at sun.ref
        | lect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)\x0A\x09at java.lang.reflect.Method.
        | invoke(Method.java:498)\x0A\x09at org.apache.spark.deploy.yarn.ApplicationMaster$$anon$3.run(ApplicationMaster.sca
        | la:686)\x0ACaused by: org.apache.spark.sql.AnalysisException: cannot resolve '`ext.mkted_prod_nm`' given input col
        | umns: [month_id, ims_pln_id, rx_written_dt, patient_eligibility, otlt_zip, cohort_desc, pharmacy_zip, cmf_pack_nbr
        | , days_supply_cnt, diag_cd, stable_pharmacy, week_id, pat_age_nbr, pat_zip3_cd, daw_cd, dacon_qty, ims_payer_id, c
        | hnl_cd, cohort_id, diag_vers_typ_id, bin, pharmacy_id, patient_id, auth_rfll_nbr, dspnsd_qty, supplier_id, ndc, sv
        | c_dt, plan_id, pat_pay_amt, total_paid_amt, rx_dosage_amt, pay_typ_cd, cust_prc, fill_nbr, pat_gender_cd, rx_typ_c
        | d, product_id, claim_id, pat_brth_yr_nbr, copay_amt]; line 11 pos 6;\x0A'Sort ['patientId ASC NULLS FIRST, 'svcDt
        | ASC NULLS FIRST, 'daysSupplyCnt ASC NULLS FIRST, 'claimId ASC NULLS FIRST], false\x0A+- 'RepartitionByExpression [
        | 'patientId], 200\x0A   +- 'Project ['patient_id AS patientId#182, 'ext.claim_id AS claimId#183, ('unix_timestamp('
        | ext.svc_dt) * 1000) AS svcDt#184, 'ext.days_supply_cnt AS daysSupplyCnt#185, cast('ext.mkted_prod_nm as string) AS
        |  pncProductGrp#186,  AS pncClassGrp#187,  AS pncMarketGrp#188]\x0A      +- 'Filter isnotnull('ext.mkted_prod_nm)\x
        | 0A         +- SubqueryAlias ext\x0A            +- SubqueryAlias rx_basic_filters_2018061913501847215\x0A
        |      +- Relation[patient_eligibility#189,rx_dosage_amt#190,pharmacy_zip#191,dacon_qty#192,pay_typ_cd#193,supplier_
        | id#194L,pat_gender_cd#195,stable_pharmacy#196,total_paid_amt#197,bin#198,chnl_cd#199,pat_zip3_cd#200,cohort_desc#2
        | 01,rx_typ_cd#202,pharmacy_id#203,otlt_zip#204,pat_brth_yr_nbr#205,pat_age_nbr#206L,patient_id#207L,plan_id#208,pro
        | duct_id#209,rx_written_dt#210,dspnsd_qty#211,copay_amt#212,... 17 more fields] parquet\x0A\x0A\x09at org.apache.sp
        | ark.sql.catalyst.analysis.package$AnalysisErrorAt.failAnalysis(package.scala:42)\x0A\x09at org.apache.spark.sql.ca
        | talyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(CheckAnalysis.scala:88)\x0A\x0
        | 9at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1$$anonfun$apply$2.applyOrElse(Che
        | ckAnalysis.scala:85)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.
        | scala:289)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$transformUp$1.apply(TreeNode.scala:289)
        | \x0A\x09at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:70)\x0A\x09at org.apache.s
        | park.sql.catalyst.trees.TreeNode.transformUp(TreeNode.scala:288)\x0A\x09at org.apache.spark.sql.catalyst.trees.Tre
        | eNode$$anonfun$3.apply(TreeNode.scala:286)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$3.apply
        | (TreeNode.scala:286)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$4.apply(TreeNode.scala:306)\x
        | 0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator(TreeNode.scala:187)\x0A\x09at org.apache.
        | spark.sql.catalyst.trees.TreeNode.mapChildren(TreeNode.scala:304)\x0A\x09at org.apache.spark.sql.catalyst.trees.Tr
        | eeNode.transformUp(TreeNode.scala:286)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transformE
        | xpressionsUp$1.apply(QueryPlan.scala:268)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan$$anonfun$transfo
        | rmExpressionsUp$1.apply(QueryPlan.scala:268)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpr
        | ession$1(QueryPlan.scala:279)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan.org$apache$spark$sql$catalys
        | t$plans$QueryPlan$$recursiveTransform$1(QueryPlan.scala:289)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPl
        | an$$anonfun$6.apply(QueryPlan.scala:298)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.mapProductIterator
        | (TreeNode.scala:187)\x0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan.mapExpressions(QueryPlan.scala:298)\x
        | 0A\x09at org.apache.spark.sql.catalyst.plans.QueryPlan.transformExpressionsUp(QueryPlan.scala:268)\x0A\x09at org.a
        | pache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:85)\x0A\x09at o
        | rg.apache.spark.sql.catalyst.analysis.CheckAnalysis$$anonfun$checkAnalysis$1.apply(CheckAnalysis.scala:78)\x0A\x09
        | at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:127)\x0A\x09at org.apache.spark.sql.catal
        | yst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at org.apache.spark.sql.catalyst.trees.Tr
        | eeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at scala.collection.immutable.List.foreach(List.scala
        | :381)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:126)\x0A\x09at org.apache.sp
        | ark.sql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at org.apache.spark.sql.cata
        | lyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at scala.collection.immutable.List.forea
        | ch(List.scala:381)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:126)\x0A\x09at
        | org.apache.spark.sql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at org.apache.s
        | park.sql.catalyst.trees.TreeNode$$anonfun$foreachUp$1.apply(TreeNode.scala:126)\x0A\x09at scala.collection.immutab
        | le.List.foreach(List.scala:381)\x0A\x09at org.apache.spark.sql.catalyst.trees.TreeNode.foreachUp(TreeNode.scala:12
        | 6)\x0A\x09at org.apache.spark.sql.catalyst.analysis.CheckAnalysis$class.checkAnalysis(CheckAnalysis.scala:78)\x0A\
        | x09at org.apache.spark.sql.catalyst.analysis.Analyzer.checkAnalysis(Analyzer.scala:91)\x0A\x09at org.apache.spark.
        | sql.execution.QueryExecution.assertAnalyzed(QueryExecution.scala:52)\x0A\x09at org.apache.spark.sql.Dataset$.ofRow
        | s(Dataset.scala:66)\x0A\x09at org.apache.spark.sql.SparkSession.sql(SparkSession.scala:623)\x0A\x09at com.rxcorp.f
        | tpa.wfengine.spark.persistencyandcompliance.tables.PatientHub$$anonfun$1.apply(PatientHub.scala:38)\x0A\x09at com.
        | rxcorp.ftpa.wfengine.spark.persistencyandcompliance.tables.PatientHub$$anonfun$1.apply(PatientHub.scala:12)\x0A\x0
        | 9at com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.execute.ExecutePNC.invokePnC(ExecutePNC.scala:68)\x0A\
        | x09at com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.transform.PnCTransformer.transform(PnCTransformer.sc
        | ala:13)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.job.Runner$$anonfun$run$1$$anonfun$com$rxcorp$ftpa$wfengine$spark
        | $job$Runner$$anonfun$$processTasks$1$1.apply(Runner.scala:26)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.job.Runner$
        | $anonfun$run$1$$anonfun$com$rxcorp$ftpa$wfengine$spark$job$Runner$$anonfun$$processTasks$1$1.apply(Runner.scala:23
        | )\x0A\x09at scala.collection.immutable.List.foreach(List.scala:381)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.job.R
        | unner$$anonfun$run$1.com$rxcorp$ftpa$wfengine$spark$job$Runner$$anonfun$$processTasks$1(Runner.scala:23)\x0A\x09at
        |  com.rxcorp.ftpa.wfengine.spark.job.Runner$$anonfun$run$1.apply(Runner.scala:49)\x0A\x09at com.rxcorp.ftpa.wfengin
        | e.spark.job.Runner$$anonfun$run$1.apply(Runner.scala:19)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.SparkSessionScop
        | e$.apply(SparkSessionScope.scala:24)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.job.Runner.run(Runner.scala:19)\x0A\
        | x09at com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$$anonfun$run$1$$anonfun$apply$1.apply$mcV$
        | sp(PnCDriver.scala:153)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnCDriver$$anonfun$run$1
        | $$anonfun$apply$1.apply(PnCDriver.scala:123)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.persistencyandcompliance.PnC
        | Driver$$anonfun$run$1$$anonfun$apply$1.apply(PnCDriver.scala:123)\x0A\x09at com.rxcorp.ftpa.wfengine.spark.JobExce
        | ptionHandler$.apply(JobExceptionHandler.scala:22)\x0A\x09... 11 more\x0A
      """.stripMargin
    val stp_8_pend_ts = "2018-06-19T13:50:37.234Z"
    val stp_8_run_ts = "2018-06-19T13:52:08.453Z"
    val stp_8_status = "failed"
    val stp_8_typ = "persis_comp"

    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("add_ts"), Bytes.toBytes(add_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("cfg"), Bytes.toBytes(cfg))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("done_ts"), Bytes.toBytes(done_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fail_msg"), Bytes.toBytes(fail_msg))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("fail_trace"), Bytes.toBytes(fail_trace))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("st"), Bytes.toBytes(st))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("steps"), Bytes.toBytes(steps))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_14_pend_ts"), Bytes.toBytes(stp_14_pend_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_14_status"), Bytes.toBytes(stp_14_status))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_14_typ"), Bytes.toBytes(stp_14_typ))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_18_attr"), Bytes.toBytes(stp_18_attr))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_18_done_ts"), Bytes.toBytes(stp_18_done_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_18_pend_ts"), Bytes.toBytes(stp_18_pend_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_18_run_ts"), Bytes.toBytes(stp_18_run_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_18_status"), Bytes.toBytes(stp_18_status))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_18_typ"), Bytes.toBytes(stp_18_typ))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_1_attr"), Bytes.toBytes(stp_1_attr))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_1_done_ts"), Bytes.toBytes(stp_1_done_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_1_pend_ts"), Bytes.toBytes(stp_1_pend_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_1_run_ts"), Bytes.toBytes(stp_1_run_ts))


    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_1_status"), Bytes.toBytes(stp_1_status))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_1_typ"), Bytes.toBytes(stp_1_typ))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_8_done_ts"), Bytes.toBytes(stp_8_done_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_8_fail_msg"), Bytes.toBytes(stp_8_fail_msg))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_8_fail_trace"), Bytes.toBytes(stp_8_fail_trace))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_8_pend_ts"), Bytes.toBytes(stp_8_pend_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_8_run_ts"), Bytes.toBytes(stp_8_run_ts))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_8_status"), Bytes.toBytes(stp_8_status))
    put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("stp_8_typ"), Bytes.toBytes(stp_8_typ))

    hTable.put(put)
    println("Record inserted Successfully...!!!")

  }
}
