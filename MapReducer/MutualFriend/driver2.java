import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class driver2 {
    /*
    实例化两个job，并分工
    利用JobControl控制job进行的先后顺序
    并利用Thread多线程运行
     */
    public static void main(String[] args) throws IOException {
        //设置文件路径
        Configuration conf = new Configuration();
        Path inputPath = new Path("./src/input");
        Path outputPath1 = new Path("./src/out1");
        Path outputPath2 = new Path("./src/out2");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath1)) {
            fs.delete(outputPath1, true);
        }
        if (fs.exists(outputPath2)) {
            fs.delete(outputPath2, true);
        }

        //实例化job
        Job job1 = Job.getInstance();
        Job job2 = Job.getInstance();

        //设置job1的参数
        job1.setMapperClass(BuddyMapper.class);
        job1.setReducerClass(BuddyReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setJobName("Buddy");
        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath1);

        //设置job2的参数
        job2.setMapperClass(MutualFriendMapper.class);
        job2.setReducerClass(MutualFriendReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setJobName("Mutual Friend");
        FileInputFormat.setInputPaths(job2, outputPath1);
        FileOutputFormat.setOutputPath(job2, outputPath2);

        //设置JobControl
        JobControl jobControl = new JobControl("Mutual Friend");
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());

        controlledJob2.addDependingJob(controlledJob1);
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);

        //开启线程
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.setDaemon(true);
        jobControlThread.start();

        while (true) {
            if (jobControl.allFinished()) {
                System.out.println(jobControl.getSuccessfulJobList());
                return;
            }
        }
    }
}
