package bfs;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class IterateBFSMapper extends Mapper<LongWritable, BFSNode, LongWritable, BFSNode> {

	// For emitting estimated distances to the outgoing nodes
	private final BFSNode distanceNode = new BFSNode();
	private LongWritable key = new LongWritable();

	
	protected void setup(Context context) throws IOException ,InterruptedException {
		//to differentiate the two types of emitted values, distanceNodes have MIN_VALUE as id
		distanceNode.setId(Long.MIN_VALUE);
	};
	
	public void map(LongWritable nid, BFSNode node,	Context context) throws IOException, InterruptedException {

		// Pass along graph structure to reducer, as well as .
		context.write(nid, node);

		if (node.getDistance() < Integer.MAX_VALUE) {
			// infinite distance: no point in estimating distances to neighbours

			// we count how many nodes have already distances lesser than infinite
			// (i.e. we know a path to them)
			context.getCounter(IterateBFS.Counters.ReachableNodesAtMap)
					.increment(1);

			// adj is the list of outgoing nodes
			long[] dest = node.getDest();
			int dist = node.getDistance() + 1;
			// Keep track of shortest distance to neighbors.
			for (int i = 0; i < dest.length; i++) {
				long neighbor = dest[i];
				distanceNode.setDistance(dist);
				
				key.set(neighbor);
				context.write(key, distanceNode);

			}
		}
	}

}