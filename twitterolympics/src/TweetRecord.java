import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class TweetRecord extends Tweet implements Writable, DBWritable {

	private final static SimpleDateFormat sdf = new SimpleDateFormat(
			"yyyy-MM-dd, hh:mm:ss");

	public static String[] fields = { "createdAt", "geoLocationLat",
			"geoLocationLong", "placeInfo", "id", "tweet", "source", "lang",
			"screenName", "replyTo", "rtCount", "hashtags" };


	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		try {

			setCreatedAt(sdf.parse(resultSet.getString("createdAt")));

		} catch (ParseException e) {
			throw new SQLException("Failed to convert String to date", e);
		}
		setGeoLocationLat(resultSet.getDouble("geoLocationLat"));
		setGeoLocationLong(resultSet.getDouble("geoLocationLong"));
	
		setPlaceInfo(resultSet.getString("placeInfo"));
		setId(resultSet.getLong("id"));
		setTweet(resultSet.getString("tweet"));
		setSource(resultSet.getString("source"));
		setLang(resultSet.getString("lang"));
		setScreenName(resultSet.getString("screenName"));
		setReplyTo(resultSet.getString("replyTo"));
		setRtCount(resultSet.getInt("rtCount"));
		setHashtags(resultSet.getString("hashtags"));
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		/*int idx = 1;
		statement.setString(idx++, getSymbol());
		try {
			statement.setDate(idx++, new Date(sdf.parse(getDate()).getTime()));
		} catch (ParseException e) {
			throw new SQLException("Failed to convert String to date", e);
		}
		statement.setDouble(idx++, getOpen());
		statement.setDouble(idx++, getHigh());
		statement.setDouble(idx++, getLow());
		statement.setDouble(idx++, getClose());
		statement.setInt(idx++, getVolume());
		statement.setDouble(idx, getAdjClose());*/
	}

	
	
}
