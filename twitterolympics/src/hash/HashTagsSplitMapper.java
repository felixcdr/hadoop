package hash;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HashTagsSplitMapper extends
		Mapper<Tweet, NullWritable, Text, IntWritable> {

	private IntWritable one = new IntWritable(1);
	private Text hashText = new Text();

	public void map(Tweet tweet, NullWritable nothing, Context context)
			throws IOException, InterruptedException {
		String field = tweet.getHashtags();
		
		if (field!=null){
			String[] hashtags = field.split(",");
					
			for (String hashtag : hashtags) {
				hashText.set(hashtag);
				context.write(hashText, one);
			}
			
		}
		
		
		

	}
}
