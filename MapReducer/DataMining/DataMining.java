<<<<<<< HEAD
package DataMining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataMining {

    public static class mapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] childAndParent = line.split(" ");
            List<String> list = new ArrayList<>(2);
            for (String name : childAndParent) {
                if (!"".equals(name)) {
                    list.add(name);
                }
            }
            if(!"child".equals(list.get(0))) {
                String childName = list.get(0);
                String parentName = list.get(1);
                String relationshipType = "1";
                context.write(new Text(parentName), new Text(relationshipType+"_"+childName+"_"+parentName));
                relationshipType = "0";
                context.write(new Text(childName), new Text(relationshipType+"_"+childName+"_"+parentName));
            }
        }
    }

    public static int time = 0;

    public static class reducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (time == 0) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            List<String> grandchild = new ArrayList<>();
            List<String> grandparent = new ArrayList<>();
            for (Text text : values) {
                String s = text.toString();
                String[] relation = s.split("\\_");
                String relationshipType = relation[0];
                String childName = relation[1];
                String parentName = relation[2];
                if("1".equals(relationshipType)) {
                    grandchild.add(childName);
                }else {
                    grandparent.add(parentName);
                }
            }
            int grandparentNum = grandparent.size();
            int grandchildNum = grandchild.size();
            if (grandparentNum != 0 && grandchildNum != 0) {
                for (int m = 0; m < grandchildNum; m++) {
                    for (int n = 0; n < grandparentNum; n++) {
                        context.write(new Text(grandchild.get(m)), new Text(grandparent.get(n)));
                    }
                }
            }
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(DataMining.mapper.class);
        job.setReducerClass(DataMining.reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        Path input = new Path("./src/DataMining/input");
        FileInputFormat.setInputPaths(job, input);
        Path output = new Path("./src/DataMining/output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
=======
package DataMining;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataMining {

    public static class mapper extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] childAndParent = line.split(" ");
            List<String> list = new ArrayList<>(2);
            for (String name : childAndParent) {
                if (!"".equals(name)) {
                    list.add(name);
                }
            }
            if(!"child".equals(list.get(0))) {
                String childName = list.get(0);
                String parentName = list.get(1);
                String relationshipType = "1";
                context.write(new Text(parentName), new Text(relationshipType+"_"+childName+"_"+parentName));
                relationshipType = "0";
                context.write(new Text(childName), new Text(relationshipType+"_"+childName+"_"+parentName));
            }
        }
    }

    public static int time = 0;

    public static class reducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (time == 0) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            List<String> grandchild = new ArrayList<>();
            List<String> grandparent = new ArrayList<>();
            for (Text text : values) {
                String s = text.toString();
                String[] relation = s.split("\\_");
                String relationshipType = relation[0];
                String childName = relation[1];
                String parentName = relation[2];
                if("1".equals(relationshipType)) {
                    grandchild.add(childName);
                }else {
                    grandparent.add(parentName);
                }
            }
            int grandparentNum = grandparent.size();
            int grandchildNum = grandchild.size();
            if (grandparentNum != 0 && grandchildNum != 0) {
                for (int m = 0; m < grandchildNum; m++) {
                    for (int n = 0; n < grandparentNum; n++) {
                        context.write(new Text(grandchild.get(m)), new Text(grandparent.get(n)));
                    }
                }
            }
        }
    }

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(DataMining.mapper.class);
        job.setReducerClass(DataMining.reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        Path input = new Path("./src/DataMining/input");
        FileInputFormat.setInputPaths(job, input);
        Path output = new Path("./src/DataMining/output");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }

}
>>>>>>> 83e7c99 (Add files via upload)
