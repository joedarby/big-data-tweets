import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashtagsJoinMapper extends Mapper<Object, Text, Text, TextIntPair> {

	private Hashtable<String, String> countryCodes = new Hashtable<String,String>();
	private TextIntPair countryTotalPair = new TextIntPair();
	private Text hashtagText = new Text();
	private enum NumberOfCodes {HASHTAG}
	
	//Map takes a "key = hashtag" and "value = occurrence count" pair from the tweets-hashtags job
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


		String line = value.toString();
		String[] splitLine = line.split("\t");
		String hashtag = splitLine[0].trim();  //trim to remove any trailing whitespace
		hashtagText.set(splitLine[0]);

		//Look at hashtag to see if it contains an IOC country code or other relevant string (eg. "TEAMGB")
		int matches = 0;
		String country = "none";
		for (String code : countryCodes.keySet()) {
			if (hashtag.startsWith(code) || hashtag.endsWith(code) || hashtag.equals(code)) {  //code must be at start of end of hashtag (to exclude eg. #GIRLS = IRELAND (IRL))
				matches += 1;
				if (matches > 1) {
					country = "none";	// If more than one country code (or other match) is made, set country = none (to exclude eg. "#GBRvsFRA")
					break;
				} else {
					country = countryCodes.get(code);   // If exactly one match is made, assign this hashtag to this country.
				}
			}
		}

		int hashtagCount = Integer.parseInt(splitLine[1]);

		countryTotalPair.set(country, hashtagCount);

		// Emit all of the original hashtags, but now labelled with the country they are *probably* supporting
		context.write(hashtagText, countryTotalPair);

	}

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		URI fileUri = context.getCacheFiles()[0];
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				context.getCounter(NumberOfCodes.HASHTAG).increment(1);

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
