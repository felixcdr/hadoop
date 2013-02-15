package stock.ecs739;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.*;

public class DailyStock implements WritableComparable<DailyStock> {

	private Text company;

	private LongWritable day;

	private DoubleWritable opening;
	private DoubleWritable close;
	private DoubleWritable high;
	private DoubleWritable low;
	private IntWritable volume;
	private DoubleWritable adjClose;

	public DailyStock() {
		set(new Text(), new LongWritable(), new DoubleWritable(),
				new DoubleWritable(), new DoubleWritable(),
				new DoubleWritable(), new IntWritable(), new DoubleWritable());
	}

	public DailyStock(String company, Calendar day, double opening,
			double close, double high, double low, double adjClose, int volume) {
		set(new Text(company), new LongWritable(day.getTimeInMillis()),
				new DoubleWritable(opening), new DoubleWritable(close),
				new DoubleWritable(high), new DoubleWritable(low),
				new IntWritable(volume), new DoubleWritable(adjClose));
	}

	public DailyStock(Text company, LongWritable day, DoubleWritable opening,
			DoubleWritable close, DoubleWritable high, DoubleWritable low,
			IntWritable volume, DoubleWritable adjClose) {
		set(company, day, opening, close, high, low, volume, adjClose);
	}

	public void set(Text company, LongWritable day, DoubleWritable opening,
			DoubleWritable close, DoubleWritable high, DoubleWritable low,
			IntWritable volume, DoubleWritable adjClose) {
		this.company = company;
		this.day = day;
		this.opening = opening;
		this.close = close;
		this.high = high;
		this.low = low;
		this.volume = volume;
		this.adjClose = adjClose;

	}

	public Text getCompany() {
		return company;
	}

	public LongWritable getDay() {
		return day;
	}

	public DoubleWritable getOpening() {
		return opening;
	}

	public DoubleWritable getClose() {
		return close;
	}

	public DoubleWritable getHigh() {
		return high;
	}

	public DoubleWritable getLow() {
		return low;
	}

	public IntWritable getVolume() {
		return volume;
	}

	public DoubleWritable getAdjClose() {
		return adjClose;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		company.write(out);
		day.write(out);
		opening.write(out);
		close.write(out);
		high.write(out);				
		low.write(out);
		volume.write(out);
		adjClose.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		company.readFields(in);
		day.readFields(in);
		opening.readFields(in);
		close.readFields(in);
		high.readFields(in);				
		low.readFields(in);
		volume.readFields(in);
		adjClose.readFields(in);
		
	}

	


	
	@Override
	public String toString() {
		return "["+ company + "," + new SimpleDateFormat("yyyy-MM-dd").format(new Date(day.get())) + "]";
	}

	@Override
	public int compareTo(DailyStock st) {
		int cmp = company.compareTo(st.getCompany());
		if (cmp != 0) {
			return cmp;
		}
		return day.compareTo(st.getDay());
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((adjClose == null) ? 0 : adjClose.hashCode());
		result = prime * result + ((close == null) ? 0 : close.hashCode());
		result = prime * result + ((company == null) ? 0 : company.hashCode());
		result = prime * result + ((day == null) ? 0 : day.hashCode());
		result = prime * result + ((high == null) ? 0 : high.hashCode());
		result = prime * result + ((low == null) ? 0 : low.hashCode());
		result = prime * result + ((opening == null) ? 0 : opening.hashCode());
		result = prime * result + ((volume == null) ? 0 : volume.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof DailyStock) {
			DailyStock st = (DailyStock) obj;
			return company.equals(st.getCompany()) && day.equals(st.getDay());
		}
		return false;
	}
	
}