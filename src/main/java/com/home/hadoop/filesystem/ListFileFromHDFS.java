package com.home.hadoop.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class ListFileFromHDFS {

    public static void main(String[] args) throws IOException {
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        FileStatus[] data= fs.listStatus(new Path(uri));
        //convert phats using the utils
        Path[] listePath = FileUtil.stat2Paths(data);
        for (Path s: listePath){
            System.out.println(s);
        }

    }
}
