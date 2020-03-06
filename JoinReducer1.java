import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class JoinReducer1 extends Reducer<TextPair, Text, Text, Text> {

	static String file_path = "movies.tsv";
	
	private MoviesMetaData metadata;
	
	
	/*  Example of loading a data file with side data distribution (distributed cache) */
	protected void setup(Context context)
			throws IOException, InterruptedException {
		try {
			metadata = new  MoviesMetaData();
			metadata.initialize(new File(file_path));
		}
		catch (Exception e) {
			e.printStackTrace();
			System.err.println("###### Exception in reducer setup: " + e.getMessage());
		}
	}
	
  @Override
  protected void reduce(TextPair key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Iterator<Text> iter = values.iterator();
    Text directorName = new Text(iter.next());
    Map<String,Set<String>> track = new HashMap<String,Set<String>>();
    while (iter.hasNext()) {
      Text record = iter.next();
      String []mapperOutput = record.toString().split("\t");
      Set<String> roles;
		 if(track.containsKey(mapperOutput[0])){
			 roles = track.get(mapperOutput[0]);
			 roles.add(mapperOutput[1]);
			 if(roles.size() == 2){
				 roles.add("dummy");
				 String value = metadata.getValue(mapperOutput[0]);
				 if(value != null){
					 context.write(directorName,new Text(value.split("\t")[0]));
				 }
			 }
			 
		 }
		 else{
			 roles = new HashSet<>();
			 roles.add(mapperOutput[1]);
			 track.put(mapperOutput[0],roles);
		 }
	     

      }      
            
    }
 
}