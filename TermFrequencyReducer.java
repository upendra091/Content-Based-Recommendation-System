package assign2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TermFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		int sumOfWordInBook = 0 ;
		for (IntWritable i : values)
		{
			sumOfWordInBook += i.get();
		}
		context.write(key, new IntWritable (sumOfWordInBook));
	}
}
