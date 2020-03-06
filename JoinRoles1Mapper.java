import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JoinRoles1Mapper extends Mapper<LongWritable, Text, TextPair, Text> {
	@Override
	
	public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		String []line = value.toString().split("\t");
		if(line[3].equals(Constants.DIRECTOR) || line[3].equals(Constants.ACTOR) || line[3].equals(Constants.ACTRESS)){
			context.write(new TextPair(line[2], "1"),new Text(line[0].trim()+"\t"+line[3]));
		}
		
	}
}