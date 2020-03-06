import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JoinNamesMapper extends Mapper<LongWritable, Text, TextPair, Text> {
	@Override

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		context.write(new TextPair(line[0], "0"), new Text(line[1]));

	}
}