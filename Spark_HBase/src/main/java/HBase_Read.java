import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by cloudwick on 6/30/2018.
 */
public class HBase_Read {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "quickstart.cloudera");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        Connection conn = ConnectionFactory.createConnection(config);

        Table table = conn.getTable(TableName.valueOf("temp"));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);

        //Iterator<Result> iter = scanner.iterator();
        for (Result record : scanner) {
            //val record = iter.next();
            System.out.println("Row Key :: " + Bytes.toString(record.getRow()));
            System.out.println("columnfamily1:col1 :: " + Bytes.toString(record.getValue(Bytes.toBytes("columnfamily1"), Bytes.toBytes("col1"))));
            System.out.println("columnfamily1:col2 :: " + Bytes.toString(record.getValue(Bytes.toBytes("columnfamily1"), Bytes.toBytes("col2"))));
        }
    }
}
