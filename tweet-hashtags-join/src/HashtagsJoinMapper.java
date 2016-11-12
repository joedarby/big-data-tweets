import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagsJoinMapper extends Mapper<Object, Text, Text, TextIntPair> {

	private Hashtable<String, String> countryCodes;
	private TextIntPair countryTotalPair = new TextIntPair();
	
	//Map takes a "key = hashtag" and "value = occurrence count" pair from the tweets-hashtags job
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] splitLine = line.split("\t");
		String hashtag = splitLine[0];

		//Look at hashtag to see if it contains an IOC country code or other relevant string (eg. "TEAMGB")
		int matches = 0;
		String country = "none";
		for (String code : countryCodes.keySet()) {
			if (hashtag.contains(code)) {
				matches += 1;
				if (matches > 1) {
					country = "none";	// If more than one country code (or other match) is made, set country = none (to exclude eg. "GBRvsFRA")
					break;
				} else {
					country = countryCodes.get(code);   // If exactly one match is made, assign this hashtag to this country.
				}
			}
		}

		countryTotalPair.set(country, splitLine[1]);

		// Emit all of the original hashtags, but now labelled with the country they are *probably* supporting
		context.write(key, countryTotalPair);

	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		countryCodes = new Hashtable<String, String>();

		URI fileUri = context.getCacheFiles()[0];
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				context.getCounter(CustomCounters.NUM_COUNTRYCODES).increment(1);

				String[] fields = line.split(",");
				if (fields.length == 2)
					countryCodes.put(fields[0], fields[1]);
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}

}
