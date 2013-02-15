package bfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class BFSNode  implements WritableComparable<BFSNode> {

	
		public static String DISTANCE_INFO = "DISTANCE_INFORMATION_ONLY";
	
	
		private Text id;

		private ArrayWritable dest;
 
		private IntWritable distance;
	

		
		public BFSNode(){
			set(new Text(),new ArrayWritable(Text.class),new IntWritable());
		}
		
		public BFSNode(Text id, ArrayWritable dest, IntWritable distance) {
			set(id, dest, distance);
		}
				
		public void set(Text id, ArrayWritable dest, IntWritable distance) {
			this.id = id;
			this.dest = dest;
			this.distance = distance;
		}
		
		public void set(String id, String[]dest, int distance){
			this.id.set(id);
			Text[] values = new Text[dest.length];
			
			for (int i = 0; i < dest.length; i++) {
				values[i] = new Text(dest[i]);
			}
							
			this.dest.set(values);
			this.distance.set(distance);
			
		}
		
		public void setId(String id){
			this.id.set(id);
		}
		
		public void setDistance(int distance){
			this.distance.set(distance);
		}

		
		public String getId() {
			return id.toString();
		}

		public String[] getDest() {
			Text[] arr = (Text[]) dest.get();
			String[] ldest = new String[arr.length];
			for (int i = 0; i < arr.length; i++) {
				ldest[i] = arr[i].toString();
			}
			return ldest;
		}

		public int getDistance() {
			return distance.get();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			id.write(out);
			dest.write(out);
			distance.write(out);
			
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			id.readFields(in);
			dest.readFields(in);
			distance.readFields(in);
			
		}

				

		@Override
		public int compareTo(BFSNode node) {
			String self = this.getId();
			
			return self.compareTo(node.getId());
		}
		
				@Override
		public boolean equals(Object obj) {
			if (obj instanceof BFSNode) {
				BFSNode node = (BFSNode) obj;
				return this.getId().equals(node.getId());
			}
			return false;
		}
		
	
}
