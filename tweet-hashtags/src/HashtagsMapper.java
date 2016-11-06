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
                ArrayList<String> hashtags = new ArrayList<String>();
                Pattern myRegex = Pattern.compile("(\\s|\\A)#([\\w\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u01FF]+)");
                Matcher matcher = myRegex.matcher(tweet);

                //add all hashtags as uppercase to ensure upper and lower are treated as the same
                while (matcher.find()) {
                    String tag = matcher.group().toUpperCase();
                    //remove any leading space
                    if (tag.startsWith(" ")) {
                        tag = tag.substring(1);
                    }
                    hashtags.add(tag);
                }

                for (String hashtag : hashtags) {
                    Text textTag = new Text();
                    textTag.set(hashtag);
                    IntWritable one = new IntWritable(1);
                    context.write(textTag, one);
                }

            }

        }

    }
        
    	
    	
}
