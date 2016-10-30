package assign2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFIDFMapper extends Mapper<LongWritable, Text, Text, Text>
{
	
	//Input word:book       countinbook/totalWordInBook
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException 
	{
		
		String lineOfFile = value.toString() ;
		String [] tokens = lineOfFile.split("\t");
		
		String word = tokens[0].split(":")[0] ;
		//output will be book:countinbook/totalwordinbook
		String restOfInfo = tokens[0].split(":")[1]+":"+tokens[1] ;
		context.write(new Text(word), new Text(restOfInfo));
		
	}
	
}
