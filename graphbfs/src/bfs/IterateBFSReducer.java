

package bfs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterateBFSReducer extends Reducer<Text, BFSNode, Text, BFSNode> {
	
	

	public void reduce(Text nid, Iterable<BFSNode> values,Context context)
			throws IOException, InterruptedException {

		BFSNode node = null;
		int dist = Integer.MAX_VALUE;
		for (BFSNode n : values) {
				
			if (n.getDistance() < dist) {
				dist = n.getDistance();
			}
			//We need the complete node to propagate graph structure
			if (n.getId().equals(BFSNode.DISTANCE_INFO) == false) {
				node = n;
			}
		}

		node.setDistance(dist); // Update the final distance.

		if (dist < Integer.MAX_VALUE) {
			context.getCounter(IterateBFS.Counters.ReachableNodesAtReduce)
					.increment(1);
		}

		context.write(nid, node);
	}

}