

package bfs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class IterateBFSReducer extends Reducer<LongWritable, BFSNode, LongWritable, BFSNode> {
	
	private BFSNode node;

	public void reduce(LongWritable nid, Iterable<BFSNode> values,Context context)
			throws IOException, InterruptedException {

		int dist = Integer.MAX_VALUE;
		for (BFSNode node : values) {
				
			if (node.getDistance() < dist) {
				dist = node.getDistance();
			}
			//We need the complete node to propagate graph structure
			if (!node.getId().equals(BFSNode.DISTANCE_INFO)) {
				this.node = node;
			}
		}

		node.setDistance(dist); // Update the final distance.

		if (dist != Long.MAX_VALUE) {
			context.getCounter(IterateBFS.Counters.ReachableNodesAtReduce)
					.increment(1);
		}

		context.write(nid, node);
	}

}