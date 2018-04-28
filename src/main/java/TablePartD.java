import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TablePartD {

    public static void main(String[] args) throws IOException {

        Configuration config = HBaseConfiguration.create();
        HTable table = new HTable(config, "powers");

        Get g = new Get(Bytes.toBytes("row1"));
        Result result = table.get(g);
        byte[] hero1 = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
        byte[] power1 = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("power"));
        byte[] name1 = result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("name"));

        byte[] xp1 = result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("xp"));

        byte[] color = result.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));

        System.out.println(
                "hero: " + Bytes.toString(hero1) +
                        ", power: " + Bytes.toString(power1) +
                        ", name: " + Bytes.toString(name1) +
                        ", xp: " + Bytes.toString(xp1) +
                        ", color: " + Bytes.toString(color));

        Get row19 = new Get(Bytes.toBytes("row19"));
        Result result19 = table.get(row19);
        byte[] hero19 = result19.getValue(Bytes.toBytes("personal"), Bytes.toBytes("hero"));
        byte[] color19 = result19.getValue(Bytes.toBytes("custom"), Bytes.toBytes("color"));

        System.out.println(
                "hero: " + Bytes.toString(hero19) +
                        ", color: " + Bytes.toString(color19));

        System.out.println(
                "hero: " + Bytes.toString(hero1) +
                        ", name: " + Bytes.toString(name1) +
                        ", color: " + Bytes.toString(color));
    }
}

