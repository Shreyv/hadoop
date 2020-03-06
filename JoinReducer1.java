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
	
	
	protected void setup(Context context)
			throws IOException, InterruptedException {
		try {
			metadata = new  MoviesMetaData();
			metadata.initialize(new File(file_path));
		}
		catch (Exception e) {
			e.printStackTrace();
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
      
      // filter actor and actress of same ids
      String check = "";
      if(!mapperOutput[1].equals(Constants.DIRECTOR)){
    	  check = mapperOutput[1].equals(Constants.ACTOR) ? Constants.ACTRESS : Constants.ACTOR;  
      }
       
		 if(track.containsKey(mapperOutput[0])){
			 roles = track.get(mapperOutput[0]);
		      if(!mapperOutput[1].equals(Constants.DIRECTOR) && !roles.contains(check)){
		    	  roles.add(mapperOutput[1]);
					 if(roles.size() == 2){ // check if size is 2 then both actor/actress  and director of same movie 
						 roles.add(Constants.DUMMY_ROLE); // adding dummy role to make it size > 2
						 String value = metadata.getValue(mapperOutput[0]);
						 if(value != null){
							 context.write(directorName,new Text(value.split("\t")[0]));
						 }
					 }
		      }
		      else if (mapperOutput[1].equals(Constants.DIRECTOR)){ 
		    	  roles.add(mapperOutput[1]);
					 if(roles.size() == 2){
						 roles.add(Constants.DUMMY_ROLE);
						 String value = metadata.getValue(mapperOutput[0]);
						 if(value != null){
							 context.write(directorName,new Text(value.split("\t")[0]));
						 }
					 }
		      }
			 
			 
		 }
		 else{
			 // creating roles set and initializing it with first value
			 roles = new HashSet<>();
			 roles.add(mapperOutput[1]);
			 track.put(mapperOutput[0],roles);
		 }
	     

      }      
            
    }
 
}