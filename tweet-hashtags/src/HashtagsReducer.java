import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class HashtagsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)

              throws IOException, InterruptedException {

        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        //only write result to file if the hashtag has a count of 10 or more - we are only interested in popular hashtags
        if (sum >= 50) {
            IntWritable output = new IntWritable(sum);
            context.write(key, output);
        }






       
        	

}

}
