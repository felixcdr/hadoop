package stock.ecs739.minmax;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import stock.ecs739.DailyStock;

public class DailyMaxMapper extends Mapper<Text, DailyStock, Text, DoubleWritable> {
	private final IntWritable one = new IntWritable(1);
	
	public void map(Text index, DailyStock value, Context context)
			throws IOException, InterruptedException {
		
		context.write(value.getCompany(), value.getClose());
		
	}
}