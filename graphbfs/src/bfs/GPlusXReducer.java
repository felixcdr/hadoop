package bfs;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class GPlusXReducer extends MapReduceBase 
	implements Reducer<IntWritable,Text, IntWritable, Text> {

	private Text result = new Text();

	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter)
			throws IOException {
		String links = "";
		while(values.hasNext()){
			links+= values.toString() + " ";
			
		}

		result.set(links);
		output.collect(key, result);
		
	}
}