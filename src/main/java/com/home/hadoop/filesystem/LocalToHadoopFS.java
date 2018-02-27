package com.home.hadoop.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class LocalToHadoopFS {
    public static void main(String[] args) throws Exception {
        String localFilePath = args[0];
        String hadoopFS = args[1];

        InputStream in = new BufferedInputStream(new FileInputStream(localFilePath));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(hadoopFS),conf);
        OutputStream out = fs.create(new Path(hadoopFS), new Progressable() {
            public void progress() {
                System.out.println(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
