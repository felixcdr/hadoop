package hash;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TwitterDullMapper extends
Mapper<Tweet, NullWritable, Text, IntWritable> {


public void map(Tweet tweet, NullWritable nothing, Context context)
	throws IOException, InterruptedException {





}