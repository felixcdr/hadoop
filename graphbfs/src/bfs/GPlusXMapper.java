package bfs;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class GPlusXMapper extends Mapper<LongWritable, Text, LongWritable, BFSNode> {

	private BFSNode node = new BFSNode();
	
	private long sourceId;
	private LongWritable key = new LongWritable();
	
		
	protected void setup(Context context) throws IOException ,InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String source = conf.get("SOURCENODE");
		sourceId = Long.parseLong(source);
		
	};
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try{
		//parse text. Format: id:dest1,dest2,dest3
		String[] parts = value.toString().split(":");
		long id = Long.parseLong(parts[0]);
		
		parts  = parts[1].split(",");
		long[] dest = new long[parts.length];
		for (int i = 0; i < parts.length; i++) {
			dest[i] = Long.parseLong(parts[i]);
		}
					
		//We set distance to 0 only if it is the source node
		int distance = id!=sourceId?Integer.MAX_VALUE:0;
		
		key.set(id);
		node.set(id, dest, distance);
		
		context.write(key, node);
		
		}catch (NumberFormatException e) {			
		}catch (NullPointerException e) {
		}catch (ArrayIndexOutOfBoundsException e) {
		}
	}
	
	
}
