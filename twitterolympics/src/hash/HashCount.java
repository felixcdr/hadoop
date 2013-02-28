package hash;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HashCount extends Configured implements Tool{

	static enum Counters {
		FaultyEntries
	};

	public int run(String[] args) throws Exception {

				
		Job job = new Job(getConf());

		Configuration conf = job.getConfiguration();
		
		conf.set("mapreduce.child.java.opts", "-Xmx2048m");
		
		job.setJarByClass(HashCount.class);

		///job.setMapperClass(HashTagsSplitMapper.class);
		job.setMapperClass(TwitterDullMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setCombinerClass(IntSumReducer.class);

		job.setNumReduceTasks(5);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		
		Path outputPath = new Path(args[1]);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);
				
		job.waitForCompletion(true);
		return 1;

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		int res = ToolRunner.run(conf, new HashCount(), otherArgs);
		System.exit(res);
	}

	

	

}
