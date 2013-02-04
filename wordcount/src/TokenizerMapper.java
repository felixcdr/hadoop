import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString().replaceAll("[^\\p{ASCII}]|\\d", "");
		
		StringTokenizer itr = new StringTokenizer(line,
				"-- \t\n\r\f,.:;?![]'\"#()$%&@*+-/");

		while (itr.hasMoreTokens()) {
			data.set(itr.nextToken().toLowerCase());
			context.write(data, one);
		}

	}
}