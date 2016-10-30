package assign2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TermFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	HashMap <String ,String > bookname = null ;
	protected void setup(Context context ) throws IOException
	{
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path pt = new Path(context.getConfiguration().get("inputpath"));
		ContentSummary cs = fs.getContentSummary(pt);
		long fileCount = cs.getFileCount();
		bookname = new HashMap <String ,String>((int) fileCount) ;
		FileStatus[] status =fs.listStatus(pt);
		System.out.println("Start");
		for(int i = 0 ; i < status.length ; i++)
		{
			bookname.put (status[i].getPath().getName(), getTitle(status[i].getPath(), context ,fs ) );
		}
		//fs.close();
		System.out.println("Done");
		
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException 
	{
		
		String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		
		String lineOfFile = value.toString() ;
		String [] wordsInLine = lineOfFile.split(" ");
		Text keyOfMap = new Text() ;
		for (int i = 0 ; i < wordsInLine.length ; i++ )
		{
			//removing punctuation and making word lower case
			String word = wordsInLine[i].toLowerCase().replaceAll("[^a-z0-9]", "") ;
			//setting KeyOfMap with word:bookname
			keyOfMap.set(word+ ":" + bookname.get(inputFileName));
			context.write(keyOfMap, new IntWritable(1) );
		}
		
	}
	String getTitle(Path input , Context context , FileSystem fs ) throws IOException
	{
		String title = "noname";
		BufferedReader br = new BufferedReader (new InputStreamReader (fs.open(input) )) ;
		String temp = br.readLine() ;
		System.out.println(temp);
		while( temp!=null )
		{
			if(temp.startsWith("Title:"))
			{
				title = temp.split(":")[1].trim() ;
				break;
			}
			temp = br.readLine() ;
		}
		
		title  = title.replace(' ','_').toLowerCase();
		
		br.close();
		return title ;
	}
	
}
