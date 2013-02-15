package adl;


import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class GraphToAdl {

	static enum Counters {
		FaultyEntries
	};
	
	
	public static void runJob(String[] input, String output) throws Exception {
		
		Configuration conf = new Configuration();
		
		
		conf.set("SOURCENODE","103973430947917397287");
		Job job = new Job(conf);
			
		
		job.setJarByClass(GraphToAdl.class);

		job.setMapperClass(GraphToAdlMapper.class);
		
		job.setNumReduceTasks(0);
		
		
		
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		

		Path outputPath = new Path(output);

		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);
		job.waitForCompletion(true);
    
  }
    public static void main(String[] args) throws Exception {
		runJob(Arrays.copyOfRange(args, 0, args.length - 1),
				args[args.length - 1]);
	}
  
}
