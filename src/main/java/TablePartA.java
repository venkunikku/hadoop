import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class TablePartA {

    public static void main(String[] args) throws IOException {
        // Configuration conf = new Configuration();

        // Connection con = ConnectionFactory.createConnection(conf);

        //Admin admin = con.getAdmin();
        Configuration con = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(con);

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("powers"));
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));
        tableDescriptor.addFamily(new HColumnDescriptor("custom"));

        HTableDescriptor tableDescriptorFood = new HTableDescriptor(TableName.valueOf("food"));
        tableDescriptorFood.addFamily(new HColumnDescriptor("nutrition"));
        tableDescriptorFood.addFamily(new HColumnDescriptor("taste"));

        admin.createTable(tableDescriptor);
        admin.createTable(tableDescriptorFood);

    }
}

