import com.google.gson.JsonObject;

public class Mapper implements MapReduceInterface  {
	public void map(String key, JsonObject value, DFS context,String file) throws IOException
	{
		// let newKey be the song title in value
		// let newValue be a subset of value
		// The new values can have the items of interest
		// Song title, year of release, duration, artist and album
		String newKey;
		JsonObject newValue;
		context.emit(newKey, newValue, file);
	}
	
	public void reduce(Integer key, JsonObject values, DFS context,String file) throws IOException
	{
		sort(values);
		context.emit(key, values, file);
	}
}