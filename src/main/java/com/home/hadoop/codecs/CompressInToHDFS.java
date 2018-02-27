package com.home.hadoop.codecs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class CompressInToHDFS {
    public static void main(String[] args) throws IOException {
        String uri = args[0];

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(path);

        if (codec == null) {
            System.out.println("No Codec found");
            System.exit(1);
        }

        //take out the prefix of the file. Example .gz or bz2
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        System.out.println("Value of output uri: " + outputUri);

        InputStream in = null;
        OutputStream out = null;

        try {
            //use codec to uncopress the stream and data in the file
            in = codec.createInputStream(fs.open(path));
            // use file system to create an output stream to write the uncompressed file.
            out = fs.create(new Path(outputUri));

            IOUtils.copyBytes(in, out, conf);
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

    }
}
