package assign2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TermFrequencyInBookReducer extends Reducer<Text, Text, Text, Text>  {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		int TotalSumOfWordsInBook = 0 ;
		//We need to count of each word + the total count as output . So we need to iterate the values twice. 
		//We cannot iterate Iterable twice so we can store the count in some temporary Data Structure like HM for O(1) access next time
		HashMap<String, Integer> wordCount = new HashMap <String , Integer > () ;
		
		for (Text value : values )
		{
			String wordAndCount = value.toString() ;
			String word = wordAndCount.split(":")[0] ;
			int count = Integer.parseInt(wordAndCount.split(":")[1]) ;
			TotalSumOfWordsInBook = TotalSumOfWordsInBook + count ;
			wordCount.put(word, count);
			
		}
		//Iterating over keys and output the entry count in book with total wordcount in book
		for (Entry<String, Integer> wordEntry : wordCount.entrySet() )
		{
			String word = wordEntry.getKey();
			int count = wordEntry.getValue();
			//We will need the totalCount and word count to calculate TFIDS so we store them in text format like
			//word:book1 12/900
			//So word count in book1 is 12 out of total 900 words
			
			context.write( new Text (word+":"+key.toString()) , new Text ( count+"/"+TotalSumOfWordsInBook)  );
		}
		
	}
}
