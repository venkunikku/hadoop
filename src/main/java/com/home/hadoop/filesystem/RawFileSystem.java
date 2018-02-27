package com.home.hadoop.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;

import java.io.IOException;

public class RawFileSystem {

    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        org.apache.hadoop.fs.FileSystem fs = new RawLocalFileSystem();
        fs.initialize(null,conf);

    }
}
