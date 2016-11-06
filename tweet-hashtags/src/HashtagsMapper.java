import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HashtagsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	    String[] splitLine = value.toString().split(";");

        if (splitLine.length == 4) {

            String tweet = splitLine[2];

            if (tweet.contains("#")) {
                ArrayList<String> hashtags = new ArrayList<>();
                Pattern myRegex = Pattern.compile("(\\s|\\A)#([\\w\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u01FF]+)");
                Matcher matcher = myRegex.matcher(tweet);

                while (matcher.find()) {
                    hashtags.add("#" + matcher.group());
                }

                for (String hashtag : hashtags) {
                    Text textTag = new Text();
                    textTag.set(hashtag);
                    IntWritable one = new IntWritable(1);
                    context.write(textTag, one);
                }

            }
            /*
            String[] hashtags = tweet.split("#");

            if (hashtags.length > 1) {

                for (int i = 1; i == hashtags.length-1; i++) {
                    Text textTag = new Text();
                    textTag.set(hashtags[i]);

                    IntWritable one = new IntWritable(1);

                    context.write(textTag, one);
                }

            }
            */

        }

    }
        
    	
    	
}
