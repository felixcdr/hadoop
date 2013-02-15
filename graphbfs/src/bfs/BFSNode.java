package bfs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class BFSNode  implements WritableComparable<BFSNode> {

		private LongWritable id;

		private ArrayWritable dest;
 
		private IntWritable distance;
	

		
		public BFSNode(){
			set(new LongWritable(),new ArrayWritable(LongWritable.class),new IntWritable());
		}
		
		public BFSNode(LongWritable id, ArrayWritable dest, IntWritable distance) {
			set(id, dest, distance);
		}
				
		public void set(LongWritable id, ArrayWritable dest, IntWritable distance) {
			this.id = id;
			this.dest = dest;
			this.distance = distance;
		}
		
		public void set(long id, long[]dest, int distance){
			this.id.set(id);
			LongWritable[] values = new LongWritable[dest.length];
			
			for (int i = 0; i < dest.length; i++) {
				values[i] = new LongWritable(dest[i]);
			}
							
			this.dest.set(values);
			this.distance.set(distance);
			
		}
		
		public void setId(long id){
			this.id.set(id);
		}
		
		public void setDistance(int distance){
			this.distance.set(distance);
		}

		
		public long getId() {
			return id.get();
		}

		public long[] getDest() {
			LongWritable[] arr = (LongWritable[]) dest.get();
			long[] ldest = new long[arr.length];
			for (int i = 0; i < arr.length; i++) {
				ldest[i] = arr[i].get();
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
			Long self = this.getId();
			
			return self.compareTo(node.getId());
		}
		
				@Override
		public boolean equals(Object obj) {
			if (obj instanceof BFSNode) {
				BFSNode node = (BFSNode) obj;
				return this.id.get() == node.getId();
			}
			return false;
		}
		
	
}
