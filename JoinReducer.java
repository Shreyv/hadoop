import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

	// filename to load data in side distribution
	static String file_path = "movies.tsv";

	private MoviesMetaData metadata;

	protected void setup(Context context) throws IOException, InterruptedException {
		try {
			metadata = new MoviesMetaData();
			metadata.initialize(new File(file_path));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		Text directorName = new Text(iter.next());
		while (iter.hasNext()) {
			Text record = iter.next();
			String[] name = directorName.toString().split("\t");
			String[] mapperOutput = record.toString().split("\t");
			String value = metadata.getValue(mapperOutput[0]);
			String[] movies;
			if (value != null) {
				movies = value.split("\t");
				if (movies[1].matches("-?\\d+") && mapperOutput[3].equals(Constants.DIRECTOR)
						&& Integer.parseInt(movies[1]) > 2010 && movies[2].contains(Constants.WESTERN)) {
					context.write(directorName, new Text(movies[0] + "\t" + movies[1]));
				}
			}

		}
	}
}