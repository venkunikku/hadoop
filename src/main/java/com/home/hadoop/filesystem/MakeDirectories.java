package com.home.hadoop.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class MakeDirectories {

    public static void main(String[] args) throws IOException{
        String directoryName = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(directoryName),conf);
        fs.mkdirs(new Path(directoryName));
    }
}
