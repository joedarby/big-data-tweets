import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	    String[] splitLine = value.toString().split(";");

        if (splitLine.length == 4) {

            try {
                long tweetTime = Long.parseLong(splitLine[0]);
                Date tweetSimpleDate = new Date(tweetTime);
                String dateString = tweetSimpleDate.getDate() + "/" + tweetSimpleDate.getMonth() + "/" + tweetSimpleDate.getYear();

                Text tweetDate = new Text();
                tweetDate.set(dateString);

                IntWritable one = new IntWritable(1);

                context.write(tweetDate, one);

            } catch (NumberFormatException e) {
                //Do nothing
            }


        }

    }
        
    	
    	
}
