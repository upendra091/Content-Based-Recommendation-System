package assign2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SimilarityMatrixMapperPhaseI extends Mapper<LongWritable, Text, Text, Text>
{
	
	//Input word:book tfidf
	//Output word book:tfidf
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException 
	{
		
		String lineOfFile = value.toString() ;
		String [] tokens = lineOfFile.split("\t");
		
		String word = tokens[0].split(":")[0] ;
		String bookName = tokens[0].split(":")[1] ;
		
		context.write(new Text(word), new Text(bookName+":"+tokens[1]));
		
	}
	
}
