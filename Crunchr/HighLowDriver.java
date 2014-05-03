import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Hadoop MapReduce example showing high and low for a stock symbol
 *
 */
public class HighLowDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String input, output;
		input = args[0];
		output = args[1];
		

		Job job = new Job(getConf());
		job.setJarByClass(HighLowDriver.class);
		job.setJobName("Get High Low for each Stock");

		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(HighLowStockMapper.class);
		job.setReducerClass(HighLowStockReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return success ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		HighLowDriver driver = new HighLowDriver();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);
	}
}