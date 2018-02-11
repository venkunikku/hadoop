package com.home.hadoop.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.net.URI;

public class FSDataInputStreamExample {

    public static void main(String[] args) throws IOException{
        String url = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(url), conf);
        System.out.println(URI.create(url).getHost());
        System.out.println("*********************************************************");
        FSDataInputStream in = null;
        try{
            in = fs.open(new Path(url));
            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0);
            System.out.println("*********************************************************");
            IOUtils.copyBytes(in, System.out, 4096, false);
        }finally {
            IOUtils.closeStream(in);
        }
    }
}
