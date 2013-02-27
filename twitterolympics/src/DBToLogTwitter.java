import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class DBToLogTwitter {

	static enum Counters {
		FaultyEntries
	};

	public static void runJob(String mysqlJar, String output) throws Exception {

		
		
		Configuration conf = new Configuration();

		JobHelper.addJarForJob(conf, mysqlJar);

		Job job = new Job(conf);

		conf = job.getConfiguration();
		
		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver",
				"jdbc:mysql://quark.student.eecs.qmul.ac.uk/tridec01"
						+ "?user=olympicro&password=olympicro");

		job.setJarByClass(DBToLogTwitter.class);

		job.setMapperClass(Map.class);

		job.setNumReduceTasks(0);

		job.setInputFormatClass(DBInputFormat.class);

		job.setOutputKeyClass(Tweet.class);
		job.setOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job,
		SequenceFile.CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job,
		DefaultCodec.class);
		
		Path outputPath = new Path(output);

		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(conf).delete(outputPath, true);
		
		DBInputFormat.setInput(
		        job,
		        TweetRecord.class,
		        "select * from T07272039BASELINEMerged limit 200",
		        "SELECT COUNT(id) FROM T07272039BASELINEMerged");
		
		
		job.waitForCompletion(true);

	}

	public static void main(String[] args) throws Exception {
		runJob(args[0], args[1]);
	}

	public static class Map extends
			Mapper<LongWritable, TweetRecord, Tweet, NullWritable> {

		public void map(LongWritable key, TweetRecord value, Context context)
				throws IOException, InterruptedException {
			context.write((Tweet) value, NullWritable.get());
		}

	}

}
