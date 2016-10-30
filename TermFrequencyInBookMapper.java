package assign2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TermFrequencyInBookMapper extends Mapper<LongWritable, Text, Text, Text>
{
	
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException 
	{
		
		String lineOfFile = value.toString() ;
		String [] tokens = lineOfFile.split("\t");
		
		
		Text keyOfMap = new Text() ;
		
		//line be like word:bookname count so split it on : and 0th index will be word and 1st be bookName
		String word = tokens[0].split(":")[0] ;
		String bookName = tokens[0].split(":")[1] ;

		//setting KeyOfMap with word:bookname
		keyOfMap.set(bookName);
		context.write(keyOfMap, new Text(word+":"+tokens[1]));

		//output bookname -> word:10
		//So we can later gather all words from a book in reducer and count its occurence and all words count in a book
		
		
	}
	
}
