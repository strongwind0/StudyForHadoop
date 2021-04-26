package date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class VisitByDate {

    private static class mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            context.write(new Text(line), new IntWritable(1));
        }
    }

    private static class myPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text visit, IntWritable intWritable, int i) {
            String date = visit.toString().split(" ")[0];
            if (date.equals("2020/07/26"))
                return 0;
            else if (date.equals("2020/07/27"))
                return 1;
            else
                return 2;
        }

    }


    private static class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        TreeMap<Text, Integer> visits = new TreeMap<Text, Integer>();
        int num = 0;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for(IntWritable i : values) {
                count += i.get();
            }
            String web = key.toString().split("\\s+")[1];
            visits.put(new Text(web), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Text, Integer>> list = new ArrayList<Map.Entry<Text, Integer>>(visits.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<Text, Integer>>() {
                @Override
                public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
                    if(o1.getValue()!=o2.getValue()) {
                        return o2.getValue()-o1.getValue();
                    }else {
                        return o1.getKey().compareTo(o2.getKey());
                    }
                }
            });
            for(Map.Entry<Text, Integer> e:list) {
                context.write(e.getKey(), new IntWritable(e.getValue()));
                num ++;
                if (num>2) {
                    break;
                }
            }
            visits.clear();
            list.clear();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setPartitionerClass(myPartitioner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(3);
        Path input = new Path("./src/date/input");
        FileInputFormat.setInputPaths(job, input);
        Path output = new Path("./src/date/output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}
