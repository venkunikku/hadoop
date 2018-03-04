package com.home.hadoop.toolrunners;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

public class ToolToolRunnerGenericOptionsParserExample extends Configured implements Tool {

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-default.xml");
        Configuration.addDefaultResource("yarn-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry: conf){
            System.out.println(entry.getKey() + " ::: "+ entry.getValue());
        }
        String fileSystem =  System.getProperty("HADOOP_USER_NAME");//conf.get("HADOOP_USER_NAME");
        System.out.println("Hadoop user name:" + fileSystem);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ToolToolRunnerGenericOptionsParserExample(), args);
        System.exit(exitCode);
    }
}
