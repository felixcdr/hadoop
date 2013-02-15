package subgraph;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import bfs.BFSNode;



public class NodesXtractorMapper extends Mapper<Text, BFSNode, Text, BFSNode> {

	
	private final BFSNode distanceNode = new BFSNode();
	private Text neighbor = new Text();

	
	protected void setup(Context context) throws IOException ,InterruptedException {
		//to differentiate the two types of emitted values, distanceNodes have MIN_VALUE as id
		distanceNode.setId(BFSNode.DISTANCE_INFO);
	};

	
	public void map(Text key, BFSNode node, Context context) throws IOException, InterruptedException {

		if(node.getDistance() < Integer.MAX_VALUE){
			
			context.write(key, node);
		
			String[] dest = node.getDest();
			int dist = node.getDistance() + 1;
			// Keep track of shortest distance to neighbors.
			for (int i = 0; i < dest.length; i++) {
				String neighborId = dest[i];
				distanceNode.setDistance(dist);
				key.set(neighborId);
				context.write(key, distanceNode);

			}
		
		}
	}
	
	
}
