import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TablePartC {

    public static void main(String[] args) throws IOException {
        String line = null;
        Configuration config = HBaseConfiguration.create();

        HTable hTable = new HTable(config, "powers");
        int i = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader("input.csv"))) {

            while ((line = reader.readLine()) != null) {
                String[] country = line.split(",");
                Put p = new Put(Bytes.toBytes(country[0]));
                i++;
                p.add(Bytes.toBytes("personal"), Bytes.toBytes("hero"), Bytes.toBytes(country[1]));
                p.add(Bytes.toBytes("personal"), Bytes.toBytes("power"), Bytes.toBytes(country[2]));

                p.add(Bytes.toBytes("professional"), Bytes.toBytes("name"), Bytes.toBytes(country[3]));
                p.add(Bytes.toBytes("professional"), Bytes.toBytes("xp"), Bytes.toBytes(country[4]));

                p.add(Bytes.toBytes("custom"), Bytes.toBytes("color"), Bytes.toBytes(country[5]));

                hTable.put(p);
            }

        }

        hTable.close();
    }
}

