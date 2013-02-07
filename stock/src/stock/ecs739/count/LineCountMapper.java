package stock.ecs739.count;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private Text data = new Text("lines");

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		context.write(data, one);
		
	}
}