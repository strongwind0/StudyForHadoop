import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class driver {

    /*
    第一种方式
    先惊醒一次job，输出到output1，即朋友关系输出
    再根据第一次job放回值，来判断是否进行第二个job，输出到output2，输出结果
    */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(BuddyMapper.class);
        job.setReducerClass(BuddyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        Path input = new Path("./src/input");
        FileInputFormat.setInputPaths(job, input);
        Path output = new Path("./src/output1");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        boolean res = job.waitForCompletion(true);
        if(res) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2);
            job2.setMapperClass(MutualFriendMapper.class);
            job2.setReducerClass(MutualFriendReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            job2.setNumReduceTasks(1);
            Path input2 = new Path("./src/output1");
            FileInputFormat.setInputPaths(job2, input2);
            Path output2 = new Path("./src/output2");
            FileSystem fs2 = FileSystem.get(conf2);
            if (fs2.exists(output2)) {
                fs2.delete(output2, true);
            }
            FileOutputFormat.setOutputPath(job2, output2);
            res = job2.waitForCompletion(true);
        }
        System.exit(res?0:1);
    }
}
