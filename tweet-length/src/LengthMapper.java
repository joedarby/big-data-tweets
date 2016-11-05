package bdp.stock;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyMaxMapper extends Mapper<Text, bdp.stock.DailyStock, Text, DoubleWritable> { 

	@Override    
	public void map(Text key, bdp.stock.DailyStock value, Context context) throws IOException, InterruptedException {

	Text company = value.getCompany();
	DoubleWritable price = new DoubleWritable();
	
	price = value.getLow();
	context.write(company, price);
	
	price = value.getHigh();
	context.write(company, price);
    	
    	
    }
        
    	
    	
}
