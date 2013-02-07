package stock.ecs739.minmax;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CompanyMinMaxReducer extends
		Reducer<Text, DoubleWritable, Text, Text> {
	private Text range = new Text();

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;
		
		for(DoubleWritable value: values){
			double d = value.get();
			if (d<min)
				min = d;
			if (d>max)
				max = d;
		}
		
		String rangeString = "MIN: " + min + "\tMAX: " + max;
		range.set(rangeString);
		context.write(key, range);
	}
}
