import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static java.util.Calendar.DAY_OF_MONTH;
import static java.util.Calendar.MONTH;
import static java.util.Calendar.YEAR;

public class TimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	    String[] splitLine = value.toString().split(";");

        if (splitLine.length == 4) {

            try {
                long tweetTime = Long.parseLong(splitLine[0]);
                Calendar cal = Calendar.getInstance();
                cal.setTimeInMillis(tweetTime);
                String dateString = cal.get(DAY_OF_MONTH) + "/" + cal.get(MONTH) + "/" + cal.get(YEAR);

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
