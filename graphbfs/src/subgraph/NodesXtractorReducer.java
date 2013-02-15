

package subgraph;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import bfs.BFSNode;

public class NodesXtractorReducer extends Reducer<Text, BFSNode, Text, BFSNode> {
	
	private BFSNode newNode = new BFSNode();

	public void reduce(Text nid, Iterable<BFSNode> values,Context context)
			throws IOException, InterruptedException {

		int dist = Integer.MAX_VALUE;
		
		boolean isNew = true;
		
		BFSNode oldNode = null;
		
		for (BFSNode node : values) {
				
			if (node.getDistance() < dist) {
				dist = node.getDistance();
			}
			//We need the complete node to propagate graph structure
			if (!node.getId().equals(BFSNode.DISTANCE_INFO)) {
				isNew = false;
				oldNode = node;
			}
		}

		
		
		
		
		if (dist < Integer.MAX_VALUE) {
			context.getCounter(NodesXtractor.Counters.ReachableNodesAtReduce)
					.increment(1);
		}
		else{
			
		}
		
		if(isNew){
			newNode.setId(nid.toString());
			newNode.setDistance(dist); // Update the final distance.

			context.write(nid, newNode);
		}
		else{//oldnode
			oldNode.setDistance(dist);
			context.write(nid, oldNode);
		}
	}

}