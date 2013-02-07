package stock.ecs739.numcompanies;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompanyMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private Text data = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String company = value.toString().split(",")[1];
		data.set(company);
		context.write(data, one);
		
	}
}