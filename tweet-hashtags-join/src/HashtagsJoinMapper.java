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

	private Hashtable<String, String> countryNames = new Hashtable<String,String>();
    private Hashtable<String, String> iocCodes = new Hashtable<String,String>();
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
		int matchFound = 0;
		String country = "none";

        /*
        for (String code : countryNames.keySet()) {
			if (hashtag.startsWith(code) || hashtag.endsWith(code) || hashtag.equals(code)) {  //code must be at start of end of hashtag (to exclude eg. #GIRLS = IRELAND (IRL))
				matches += 1;
				if (matches > 1) {
					country = "none";	// If more than one country code (or other match) is made, set country = none (to exclude eg. "#GBRvsFRA")
					break;
				} else {
					country = countryNames.get(code);   // If exactly one match is made, assign this hashtag to this country.
				}
			}
		}
		*/
        // if hashtag exactly matches an IOC country code, match this tag to the relevant country
        for (String code : iocCodes.keySet()) {
            if (hashtag.equals(code)) {
                country = iocCodes.get(code);
                matchFound = 1;
                break;
            }
        }

        //if no match is found yet, move onto the other list (country names and strings like "TEAMGB") and see if the hashtag contains the first 5 characters.
        if (matchFound == 0) {
            for (String string : countryNames.keySet()) {
                String first5 = "";
                if (string.length() >= 5) {
                    first5 = string.substring(0,4);
                } else {
                    first5 = string;
                }
                if (hashtag.contains(first5)) {
                    country = countryNames.get(string);
                    break;
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

		URI iocFileURI = context.getCacheFiles()[0];
        URI otherFileURI = context.getCacheFiles()[1];
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(iocFileURI));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				context.getCounter(NumberOfCodes.HASHTAG).increment(1);

				String[] fields = line.split(",");
				if (fields.length == 2)
					iocCodes.put(fields[0].trim(), fields[1].trim());
			}
			br.close();
		} catch (IOException e1) {
		}

        FSDataInputStream in2 = fs.open(new Path(otherFileURI));
        BufferedReader br2 = new BufferedReader(new InputStreamReader(in2));

        line = null;
        try {
            // we discard the header row
            br2.readLine();

            while ((line = br2.readLine()) != null) {
                context.getCounter(NumberOfCodes.HASHTAG).increment(1);

                String[] fields = line.split(",");
                if (fields.length == 2)
                    countryNames.put(fields[0].trim(), fields[1].trim());
            }
            br2.close();
        } catch (IOException e1) {
        }



		super.setup(context);
	}

}
