import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BuddyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] buddy = line.split(":");
        String user = buddy[0];
        buddy = buddy[1].split(",");
        for (String var : buddy) {
            context.write(new Text(var), new Text(user));
        }
    }
}
