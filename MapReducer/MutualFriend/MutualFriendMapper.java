import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class MutualFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\\s+");
        String[] buddy = lines[1].split(",");
        Arrays.sort(buddy);
        for (int i = 0; i < buddy.length; i++) {
            for (int j = i+1; j < buddy.length; j++) {
                context.write(new Text(buddy[i] + "-" + buddy[j]), new Text(lines[0]));
            }
        }
    }
}
