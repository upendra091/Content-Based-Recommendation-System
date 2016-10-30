package assign2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityMatrixReducerPhaseII extends Reducer<Text, Text, Text, Text>  {
	
	
	
	//input book1:book2 differentInTFIDF
	//output book1:book2 euclideandistance
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		double sumOfWordInBook = 0 ;
		for (Text i : values)
		{
			sumOfWordInBook += Double.parseDouble(i.toString());
		}
		context.write(key, new Text ( String.format("%.12f", (sumOfWordInBook) ) ) ) ;
	}
}
