import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FlowStatisticsTest {

    // 用于存储作业输出的路径
    private static final Path TEST_OUTPUT_PATH = new Path("/tmp/FlowStatisticsTest/output");

    @BeforeEach
    public void setUp() throws Exception {
        // 清理之前的测试输出
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(TEST_OUTPUT_PATH, true);
    }

    @Test
    public void testMapReduceJob() throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");

        // 设置作业
        Job job = Job.getInstance(conf, "Flow Statistics Test");
        job.setJarByClass(FlowStatisticsTest.class);
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 设置输入输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Access.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, TEST_OUTPUT_PATH);

        // 执行作业
        boolean result = job.waitForCompletion(true);
        Assert.assertTrue("MapReduce job failed!", result);

        // 打印输出结果
        printOutput(TEST_OUTPUT_PATH, conf);
    }

    @AfterEach
    public void tearDown() throws Exception {
        // 清理测试输出
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        fs.delete(TEST_OUTPUT_PATH, true);
    }

    private void printOutput(Path path, Configuration conf) throws IOException {
        // 读取输出文件并打印
        FileSystem fs = FileSystem.get(conf);
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                fs.open(path),
                "UTF-8"
        ));

        String line = reader.readLine();
        while (line != null) {
            System.out.println(line);
            line = reader.readLine();
        }
        reader.close();
    }

    // 其他代码...
}