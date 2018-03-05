package test;

import com.home.hadoop.mp2.toptiles.TopTitles;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TopTitlesTest {



    //@Test
    public void testTopTitles() throws IOException, InterruptedException, Exception {
        String stopwordsPath = "stopwords.txt";
        String delimterPath = "delimiters.txt";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        conf.setInt("mapreduce.task.io.sort.mb",1);
        conf.set("stopwords","Users/venkuburagadda/hadoop/hadoop_config_file/sample_data/mp2_input/"+ stopwordsPath);
        conf.set("delimiters","Users/venkuburagadda/hadoop/hadoop_config_file/sample_data/mp2_input/"+ delimterPath);

        Path input = new Path("/Users/venkuburagadda/hadoop/hadoop_config_file/sample_data/mp2_input/input.txt");
        Path output = new Path("/Users/venkuburagadda/hadoop/hadoop_config_file/sample_data/output_mp2");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output,true);

        TopTitles title = new TopTitles();
        title.run(new String[]{
                input.toString(), output.toString()
        });
    }
}
