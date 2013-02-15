package adl;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class GraphToAdlMapper extends Mapper<LongWritable, Text, Text, BFSNode> {

	private BFSNode node = new BFSNode();
	
	private String sourceId;
	private Text nodeId = new Text();
	
		
	protected void setup(Context context) throws IOException ,InterruptedException {
		
		Configuration conf = context.getConfiguration();
		sourceId = conf.get("SOURCENODE");
		
		
	};
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		try{
		//parse text. Format: id:dest1,dest2,dest3
		String[] parts = value.toString().split(":");
		String id = parts[0];
		
		String[] dest;
		
		if(parts.length ==2){
		
		dest  = parts[1].split(",");
		}else{
			dest = new String[0];
		}
			
					
		//We set distance to 0 only if it is the source node
		int distance = !id.equals(sourceId)?Integer.MAX_VALUE:0;
		
		nodeId.set(id);
		node.set(id, dest, distance);
		
		context.write(nodeId, node);
		
		}catch (NumberFormatException e) {		
			context.getCounter(GraphToAdl.Counters.FaultyEntries).increment(1);
		}catch (NullPointerException e) {
			context.getCounter(GraphToAdl.Counters.FaultyEntries).increment(1);
		}catch (ArrayIndexOutOfBoundsException e) {
			context.getCounter(GraphToAdl.Counters.FaultyEntries).increment(1);
		}
	}
	
	
}
