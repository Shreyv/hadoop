import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IOUtils;

//DATASET to store movies data in side data distribution
public class MoviesMetaData {

	private Map<String, String> idToValue = new HashMap<String, String>();

	public void initialize(File file) throws IOException {
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line;
			while ((line = in.readLine()) != null) {
				String[] values = line.split("\t", 2);
				idToValue.put(values[0], values[1]);
			}

		} finally {
			IOUtils.closeStream(in);
		}
	}

	public String getValue(String id) {
		String value = idToValue.get(id);
		if (value == null || value.trim().length() == 0) {
			return null;
		}
		return value;
	}

	public Map<String, String> getIdToNameMap() {
		return Collections.unmodifiableMap(idToValue);
	}

}