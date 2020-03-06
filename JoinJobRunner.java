import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class JoinJobRunner extends Configured implements Tool {

	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 4) {
			return -1;
		}

		Job job = new Job(getConf(), "Join weather records with station names");
		job.setJarByClass(getClass());

		Path ncdcInputPath = new Path(args[0]);
		Path stationInputPath = new Path(args[1]);
		Path outputPath = new Path(args[2]);
		DistributedCache.addCacheFile(new URI(args[3]), job.getConfiguration());

		MultipleInputs.addInputPath(job, ncdcInputPath, TextInputFormat.class, JoinNamesMapper.class);
		MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, JoinRolesMapper.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(TextPair.FirstComparator.class);

		job.setMapOutputKeyClass(TextPair.class);

		job.setReducerClass(JoinReducer.class);

		job.setOutputKeyClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinJobRunner(), args);
		System.exit(exitCode);
	}
}