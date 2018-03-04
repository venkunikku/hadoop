package test;

import com.home.hadoop.MaxTemperatureRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.CoreMatchers;
import org.junit.*;

public class MaxTemperatureTextLocalConfig {

    @Test
    public void test() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        conf.setInt("mapreduce.task.io.sort.mb",1);

        Path input = new Path("/Users/venkuburagadda/hadoop/hadoop_config_file/sample_data/ndc_temperature.txt");
        Path output = new Path("/Users/venkuburagadda/hadoop/hadoop_config_file/sample_data/output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output,true);

        MaxTemperatureRunner runner = new MaxTemperatureRunner();
        runner.setConf(conf);
        int exitCode = runner.run(new String[]{
                input.toString(), output.toString()
        });
        Assert.assertThat(exitCode, CoreMatchers.is(0));

        //checkOutput(conf, output);
    }

    private void checkOutput(Configuration conf, Path output) {

    }

}
