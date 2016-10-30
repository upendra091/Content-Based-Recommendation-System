package assign2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SimilarityMatrixMapperPhaseII extends Mapper<LongWritable, Text, Text, Text>
{
	
	//Input book1:book2 difference
	//Output book1:book2 difference
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException 
	{
		
		String lineOfFile = value.toString() ;
		String [] tokens = lineOfFile.split("\t");
		context.write(new Text(tokens[0]), new Text(tokens[1]));
		
	}
	
}
