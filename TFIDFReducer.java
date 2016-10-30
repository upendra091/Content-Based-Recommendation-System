package assign2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TFIDFReducer extends Reducer<Text, Text, Text, Text>  {
	
	
	//input will be word word ->  book:countinbook/totalwordinbook
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{

		long numberOfDocumentsInCorpus = context.getConfiguration().getLong("filecount", 0) ;
		int numberOfDocsWhereWordAppears = 0 ;
		HashMap<String, String> termsFrequencies = new HashMap <String, String> ();
		for (Text value : values )
		{
			String []bookAndCount = value.toString().split(":");
			int countInBook = Integer.parseInt(bookAndCount[1].split("/")[0]) ;
			if(countInBook>0)
			{
				numberOfDocsWhereWordAppears++ ;
			}
			//book > countinbook/totalwordinbook
			termsFrequencies.put(bookAndCount[0],bookAndCount[1]) ;
		
		}
		
		
		for (Entry<String, String> termFrequency : termsFrequencies.entrySet() )
		{
			int countInBook = Integer.parseInt(termFrequency.getValue().split("/")[0] ) ;
			int totalWordsInBook = Integer.parseInt(termFrequency.getValue().split("/")[1] ) ;
			double tf = ( (double) countInBook )/ ( (double) totalWordsInBook ) ;
			
			int normalization = 0;
			if( numberOfDocsWhereWordAppears == 0 )
			{
				normalization = 1 ;
			}
			
			double idfBeforeLog = ( (double) numberOfDocumentsInCorpus ) / ( (double) (numberOfDocsWhereWordAppears+normalization) );
			
			double idf = Math.log10(idfBeforeLog);
			
			double tfidf = tf * idf ;
			// word:bookname tfidf
			context.write(new Text (key.toString() + ":" + termFrequency.getKey() ), new Text(String.format("%.12f",tfidf)));
			
		}
	}
}
