import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HashtagsJoinJob {
    public static void runJob(String[] input, String output) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJarByClass(HashtagsJoinJob.class);
        job.setMapperClass(HashtagsJoinMapper.class);
        //no reducer - map only job
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TextIntPair.class);

        job.addCacheFile(new Path("/user/jd306/input/ioc-3lettercodes.csv").toUri());
        job.addCacheFile(new Path("/user/jd306/input/other-codes.csv").toUri());

	    job.setNumReduceTasks(1);
        
        Path outputPath = new Path(output);
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        job.waitForCompletion(true);
        }
    public static void main(String[] args) throws Exception {
        runJob(Arrays.copyOfRange(args, 0, args.length - 1), args[args.length - 1]);
    }
}
