package adl;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import bfs.BFSNode;



public class GraphToAdlMapper extends Mapper<Text, BFSNode, Text, NullWritable> {

	Text out = new Text();
	
	
	public void map(Text key, BFSNode node, Context context) throws IOException, InterruptedException {

		StringBuilder output = new StringBuilder();
		output.append(node.getId());
		output.append(":");
		String[] dest = node.getDest();
		if(dest.length>0){
			for (int i = 0; i < dest.length; i++) {
				output.append(dest[i]);
				if (i<dest.length-1)
					output.append(",");
			}
		}
		
			
		out.set(output.toString());
		context.write(out,NullWritable.get());
		
	}
	
	
}
