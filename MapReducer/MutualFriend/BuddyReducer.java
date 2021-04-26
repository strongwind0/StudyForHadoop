import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BuddyReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder buddy = new StringBuilder();
        boolean isExist = false;
        for (Text value : values) {
            if(isExist) {
                buddy.append(","+value.toString());
            }else {
                isExist = true;
                buddy.append(value.toString());
            }
        }
        context.write(key, new Text(buddy.toString()));
    }
}
