import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LengthMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	    String[] splitLine = value.toString().split(";");

        if (splitLine.length == 4) {

            String tweetContent = splitLine[2];
            int tweetLength = tweetContent.length();
            int lower = (tweetLength / 5) * 5;
            int upper = lower + 4;

            String groupName = lower + " to " + upper;

            Text group = new Text();
            group.set(groupName);

            IntWritable one = new IntWritable(1);

            context.write(group, one);
        }

    }
        
    	
    	
}
