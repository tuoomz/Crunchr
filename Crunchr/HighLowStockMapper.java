import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HighLowStockMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String input = value.toString();

		if (input.startsWith("exchange,")) {
			return;
		}

		String[] columns = input.split(",");

		if (columns.length != 9) {
			return;
		}

		double close = Double.parseDouble(columns[8]);
		context.write(new Text(columns[1]), new DoubleWritable(close));
	}
}
