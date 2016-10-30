package assign2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SimilarityMatrixReducerPhaseI extends Reducer<Text, Text, Text, Text>  {
	
	ArrayList <String> bookNames = null ;
	protected void setup(Context context ) throws IOException
	{
		//fs.close();
		System.out.println("Done");
		 	FileSystem fs = FileSystem.get(context.getConfiguration());
		    Path pt = new Path(context.getConfiguration().get("inputpath"));
		    ContentSummary cs = fs.getContentSummary(pt);
		    long fileCount = cs.getFileCount();
		    bookNames = new ArrayList <String>((int) fileCount) ;
		    FileStatus[] status =fs.listStatus(pt);
		    for(int i = 0 ; i < status.length ; i++)
		    {
		    	bookNames.add(getTitle(status[i].getPath(), context ,fs ));
		    }
		    
	}
	//input word book:tfidf 
	//output book1:book2 differentInTFIDF
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		
		HashMap<String, String> bookTfidf = new HashMap <String, String> ();
		for (Text value : values )
		{
			String []bookAndCount = value.toString().split(":");
			
			bookTfidf.put(bookAndCount[0],bookAndCount[1]) ;
		}
		for (int i = 0 ; i < bookNames.size() ; i++)
		{
			for (int j = 0 ; j < bookNames.size() ; j++ )
			{
				double tfidf_a = 0 ;
				double tfidf_b = 0 ;
				if(i == j)
				{
					continue ;
				}
				if(bookTfidf.containsKey(bookNames.get(i)))
				{
					tfidf_a = Double.parseDouble(  bookTfidf.get(bookNames.get(i))   );
				}
				if(bookTfidf.containsKey(bookNames.get(j)))
				{
					tfidf_b = Double.parseDouble(  bookTfidf.get(bookNames.get(j))   );
				}
				context.write(new Text( bookNames.get(i)+":"+bookNames.get(j) ), new Text( String.format("%.12f",Math.pow(tfidf_a - tfidf_b, 2)) ) );
				
			}
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
