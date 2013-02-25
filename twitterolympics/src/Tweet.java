import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class Tweet implements WritableComparable<Tweet> {

	private LongWritable createdAt;
	private DoubleWritable geoLocationLat;
	private DoubleWritable geoLocationLong;
	private Text placeInfo;
	private LongWritable id;
	private Text tweet;
	private Text source;
	private Text lang;
	private Text screenName;
	private Text replyTo;
	private IntWritable rtCount;
	private Text hashtags;

	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		createdAt.set(in.readLong());
		geoLocationLat.set(in.readDouble());
		geoLocationLong.set(in.readDouble());
		placeInfo.set(WritableUtils.readString(in));
		id.set(in.readLong());
		tweet.set(WritableUtils.readString(in));
		source.set(WritableUtils.readString(in));
		lang.set(WritableUtils.readString(in));
		screenName.set(WritableUtils.readString(in));
		replyTo.set(WritableUtils.readString(in));
		rtCount.set(in.readInt());
		hashtags.set(WritableUtils.readString(in));

	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(createdAt.get());
		out.writeDouble(geoLocationLat.get());
		out.writeDouble(geoLocationLong.get());
		WritableUtils.writeString(out, placeInfo.toString());
		out.writeLong(id.get());
		WritableUtils.writeString(out, tweet.toString());
		WritableUtils.writeString(out, source.toString());
		WritableUtils.writeString(out, lang.toString());
		WritableUtils.writeString(out, screenName.toString());
		WritableUtils.writeString(out, replyTo.toString());
		WritableUtils.writeString(out, rtCount.toString());
		WritableUtils.writeString(out, hashtags.toString());

	}

	public Date getCreatedAt() {
		return new Date(createdAt.get());
	}

	public void setCreatedAt(Date date) {
		this.createdAt.set(date.getTime());
	}

	public double getGeoLocationLat() {
		return geoLocationLat.get();
	}

	public void setGeoLocationLat(double geoLocationLat) {
		this.geoLocationLat.set(geoLocationLat);
	}

	public double getGeoLocationLong() {
		return geoLocationLong.get();
	}

	public void setGeoLocationLong(double geoLocationLong) {
		this.geoLocationLong.set(geoLocationLong);
	}

	public String getPlaceInfo() {
		return placeInfo.toString();
	}

	public void setPlaceInfo(String placeInfo) {
		this.placeInfo.set(placeInfo);
	}

	public long getId() {
		return id.get();
	}

	public void setId(long id) {
		this.id.set(id);
	}

	public String getTweet() {
		return tweet.toString();
	}

	public void setTweet(String tweet) {
		this.tweet.set(tweet);
	}

	public String getSource() {
		return source.toString();
	}

	public void setSource(String source) {
		this.source.set(source);
	}

	public String getLang() {
		return lang.toString();
	}

	public void setLang(String lang) {
		this.lang.set(lang);
	}

	public String getScreenName() {
		return screenName.toString();
	}

	public void setScreenName(String screenName) {
		this.screenName.set(screenName);
	}

	public String getReplyTo() {
		return replyTo.toString();
	}

	public void setReplyTo(String replyTo) {
		this.replyTo.set(replyTo);
	}

	public int getRtCount() {
		return rtCount.get();
	}

	public void setRtCount(int rtCount) {
		this.rtCount.set(rtCount);
	}

	public String getHashtags() {
		return hashtags.toString();
	}

	public void setHashtags(String hashtags) {
		this.hashtags.set(hashtags);
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((createdAt == null) ? 0 : createdAt.hashCode());
		result = prime * result
				+ ((geoLocationLat == null) ? 0 : geoLocationLat.hashCode());
		result = prime * result
				+ ((geoLocationLong == null) ? 0 : geoLocationLong.hashCode());
		result = prime * result
				+ ((hashtags == null) ? 0 : hashtags.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((lang == null) ? 0 : lang.hashCode());
		result = prime * result
				+ ((placeInfo == null) ? 0 : placeInfo.hashCode());
		result = prime * result + ((replyTo == null) ? 0 : replyTo.hashCode());
		result = prime * result + ((rtCount == null) ? 0 : rtCount.hashCode());
		result = prime * result
				+ ((screenName == null) ? 0 : screenName.hashCode());
		result = prime * result + ((source == null) ? 0 : source.hashCode());
		result = prime * result + ((tweet == null) ? 0 : tweet.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Tweet) {
			Tweet tr = (Tweet) obj;
			return id.equals(tr.getId());
		}
		return false;
	}

	@Override
	public int compareTo(Tweet otherTweet) {
		
		long thisDate = this.createdAt.get();
		
		long otherDate = otherTweet.getCreatedAt().getTime();
		
		if (thisDate > otherDate)
			return 1;
		else if (thisDate == otherDate)
			return 0;
		else 
			return -1;
		
		
	}
	
	
}
