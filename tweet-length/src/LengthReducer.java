package bdp.stock;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class LengthReducer extends Reducer<Text, DoubleWritable, Text, Text> {


    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)

              throws IOException, InterruptedException {

        double max = 0.0;
        double min = 99999.0;

        for (DoubleWritable value : values) {

            if (value.get() > max) {
            	max = value.get();
            }
            if (value.get() < min) {
            	min = value.get();
            }

        }
        
        String print = " MIN: " + min + " MAX: " + max;
        
        Text output = new Text();
        output.set(print);
        

        context.write(key, output);
       
        	

}

}
