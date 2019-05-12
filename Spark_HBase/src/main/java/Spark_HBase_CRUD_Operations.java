import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by cloudwick on 6/19/2018.
 */
public class Spark_HBase_CRUD_Operations {

    public static void getAllTables(Connection con1) throws IOException {
        HTableDescriptor tables[] = con1.getAdmin().listTables();
        int len = tables.length;
        System.out.println("Total Number of Tables in HBase ::: " + len);
        System.out.println("HBase Tables List");
        for(int i = 0; i < len; i++) {
            String tabName = tables[i].getNameAsString();
            System.out.println("Table Name ::: " + tabName);
        }
        return;
    }

    public static void createHBaseTable(Connection con1, String newTableName, List<String> colFamilies) throws IOException {
        HTableDescriptor tab = new HTableDescriptor(TableName.valueOf(newTableName));
        for(String colFamily : colFamilies) {
            tab.addFamily(new HColumnDescriptor(colFamily));
        }
        con1.getAdmin().createTable(tab);
    }

    public static void scanData(Connection con1, String tabName) throws IOException {
        Table table = con1.getTable(TableName.valueOf(tabName));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        HColumnDescriptor colFamilies[] = table.getTableDescriptor().getColumnFamilies();
        //String colFamily = "columnfamily1";
        String colName = "col1";
        for(Result res : scanner) {
            for(HColumnDescriptor colFamily : colFamilies) {
                String value = Bytes.toString(res.getValue(Bytes.toBytes(colFamily.getNameAsString()), Bytes.toBytes(colName)));
                System.out.println("Table Name :: " + tabName + "\nColumn Family :: " + colFamily +
                        "\nColumn Name :: " + colName + "\nValue :: " + value);
            }
        }
        return;
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "quickstart.cloudera");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        System.out.println("Created HBase Configuration");
        //HBaseAdmin admin = new HBaseAdmin(conf);
        Connection conn = ConnectionFactory.createConnection(conf);
        System.out.println("HBase Connection Established");
        getAllTables(conn);
        scanData(conn, "temp");
        System.out.println("Closing HBase Connection");
        conn.close();
    }
}
