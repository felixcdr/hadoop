package stock.ecs739;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;


public class StockInputFormat extends FileInputFormat<Text,DailyStock> {


	public RecordReader<Text, DailyStock> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context) {
		return new StockRecordReader();
	}

	/**
	 * Modified LineRecordReader (default one automatically parsing the
	 * StockInput data)
	 * 
	 * @author fcuadrado
	 * 
	 */

	public class StockRecordReader extends RecordReader<Text, DailyStock> {
	
		private CompressionCodecFactory compressionCodecs = null;
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		private int maxLineLength;

		private Text key = null;
		private DailyStock value = null;

		private Text line = new Text();

		// internal fields for daily stock
		private Text company = new Text();

		private LongWritable day = new LongWritable();

		private DoubleWritable opening = new DoubleWritable();
		private DoubleWritable close = new DoubleWritable();
		private DoubleWritable high = new DoubleWritable();
		private DoubleWritable low = new DoubleWritable();
		private IntWritable volume = new IntWritable();
		private DoubleWritable adjClose = new DoubleWritable();

		public void initialize(InputSplit genericSplit,
				TaskAttemptContext context) throws IOException {
			FileSplit split = (FileSplit) genericSplit;
			Configuration job = context.getConfiguration();
			this.maxLineLength = job.getInt(
					"mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			final CompressionCodec codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());
			boolean skipFirstLine = false;
			if (codec != null) {
				in = new LineReader(codec.createInputStream(fileIn), job);
				end = Long.MAX_VALUE;
			} else {
				if (start != 0) {
					skipFirstLine = true;
					--start;
					fileIn.seek(start);
				}
				in = new LineReader(fileIn, job);
			}
			if (skipFirstLine) { // skip first line and re-establish "start".
				start += in.readLine(new Text(), 0,
						(int) Math.min((long) Integer.MAX_VALUE, end - start));
			}
			this.pos = start;
		}

		public boolean nextKeyValue() throws IOException {
			if (key == null) {
				key = new Text();
			}

			if (value == null) {
				value = new DailyStock();
			}
			int newSize = 0;

			while (pos < end) {
				newSize = in.readLine(line, maxLineLength, Math.max(
						(int) Math.min(Integer.MAX_VALUE, end - pos),
						maxLineLength));
				if (newSize == 0) {
					break;
				}

				// fields:
				// exchange,stock_symbol,date,stock_price_open,stock_price_high,stock_price_low,stock_price_close,stock_volume,stock_price_adj_close
				String[] fields = line.toString().split(",");

				// data must be correctly formed
				if (fields == null || fields.length != 9) {
					break;
				}
				// parse key
				// key is the first field, pointing the stock market
				key.set(fields[0]);

				company.set(fields[1]);

				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

				// setting day by parsing from
				try {
					day.set(sdf.parse(fields[2]).getTime());

					opening.set(Double.parseDouble(fields[3]));
					close.set(Double.parseDouble(fields[4]));
					high.set(Double.parseDouble(fields[5]));
					low.set(Double.parseDouble(fields[6]));
					volume.set(Integer.parseInt(fields[7]));
					adjClose.set(Double.parseDouble(fields[8]));

					value.set(company, day, opening, close, high, low, volume,
							adjClose);

				} catch (ParseException e) {
					break;

				} catch (NumberFormatException e) {
					break;
				}
				//

				pos += newSize;
				if (newSize < maxLineLength) {
					break;
				}

				}
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

		@Override
		public Text getCurrentKey() {
			return key;
		}

		@Override
		public DailyStock getCurrentValue() {
			return value;
		}

		/**
		 * Get the progress within the split
		 */
		public float getProgress() {
			if (start == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - start) / (float) (end - start));
			}
		}

		public synchronized void close() throws IOException {
			if (in != null) {
				in.close();
			}
		}
	}
	
	@Override
	  protected boolean isSplitable(JobContext context, Path file) {
	    CompressionCodec codec = 
	      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
	    return codec == null;
	  }

}