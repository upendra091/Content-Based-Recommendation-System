package assign2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class OfflineRunner {
	
	//input_directory OutputTempDirectory outputonlocaldirectory
	public static void main ( String args[] )
	{
		if (args.length != 3)
		{
			System.out.println("There should be three parameters like Input_Directoy Temp_Directory Output_Directory ");
			return ;
		}
		try
		{
			Configuration conf = new Configuration();
			conf.set("inputpath", args[0]);
			//The Map Reduce job fails if output directory exist. So deleting output and temp directories
			FileSystem.get(conf).delete(new Path(args[1]),true);
			//Initialization of mapreduce job to find terms and their frequency
			Job TermFrequencyJob = new Job(conf, "Term Frequency");
			TermFrequencyJob.setJarByClass(OfflineRunner.class);
			TermFrequencyJob.setMapperClass(TermFrequencyMapper.class);
			TermFrequencyJob.setReducerClass(TermFrequencyReducer.class);
		    TermFrequencyJob.setOutputKeyClass(Text.class);
		    TermFrequencyJob.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(TermFrequencyJob, new Path(args[0]));
		    
		    
		    String temporaryOutputPath = args[1] + "/termfrequencycounter" ;
		    FileOutputFormat.setOutputPath(TermFrequencyJob, new Path(temporaryOutputPath ));
		    
		    
		    
		    //Starting job and wait for its completion
		    if(TermFrequencyJob.waitForCompletion(true))
		    {
		    	System.out.println( "Term Frequency Completed Successfully ");
		    }
		    else
		    {
		    	System.out.println("Term Frequency Job Failed");
		    	return ;
		    }
		    
		    
		    //Initiliazation of mapreduce job to find count of a term in a book and total count of terms in a book
		    Job TermFrequencyInBookJob = new Job(conf, "Term Frequency");
		    TermFrequencyInBookJob.setJarByClass(OfflineRunner.class);
		    TermFrequencyInBookJob.setMapperClass(TermFrequencyInBookMapper.class);
		    TermFrequencyInBookJob.setReducerClass(TermFrequencyInBookReducer.class);
		    TermFrequencyInBookJob.setOutputKeyClass(Text.class);
		    TermFrequencyInBookJob.setOutputValueClass(Text.class);
		    
		    //Make output of previous job as input of this job
		    FileInputFormat.addInputPath(TermFrequencyInBookJob, new Path(temporaryOutputPath));
		    
		    
		    temporaryOutputPath = args[1] + "/termfrequencyinbookcounter" ;
		    FileOutputFormat.setOutputPath(TermFrequencyInBookJob, new Path(temporaryOutputPath ));
		    
		    
		    
		    //Starting job and wait for its completion
		    if(TermFrequencyInBookJob.waitForCompletion(true))
		    {
		    	System.out.println( "Term Frequency Count In Book Completed Successfully ");
		    }
		    else
		    {
		    	System.out.println("Term Frequency Count in Book Job Failed");
		    	return ;
		    }
		    
		    
		    /*
		     * We need count of all files in HDFS in input directory to count number of books so that it can be used to calculate tfidf
		     */
			
		    FileSystem fs = FileSystem.get(conf);
		    Path pt = new Path(args[0]);
		    ContentSummary cs = fs.getContentSummary(pt);
		    long fileCount = cs.getFileCount();
		    conf.setLong("filecount", fileCount);
		    
		    
		    
		    //job to find TFIDF
		  //Initiliazation of mapreduce job to find count of a term in a book and total count of terms in a book
		    Job TFIDF = new Job(conf, "Term Frequency");
		    TFIDF.setJarByClass(OfflineRunner.class);
		    TFIDF.setMapperClass(TFIDFMapper.class);
		    TFIDF.setReducerClass(TFIDFReducer.class);
		    TFIDF.setOutputKeyClass(Text.class);
		    TFIDF.setOutputValueClass(Text.class);
		    
		    //Make output of previous job as input of this job
		    FileInputFormat.addInputPath(TFIDF, new Path(temporaryOutputPath));
		    temporaryOutputPath = args[1] + "/tfidf" ;
		    FileOutputFormat.setOutputPath(TFIDF, new Path(temporaryOutputPath ));
		    
		    
		    
		    //Starting job and wait for its completion
		    if(TFIDF.waitForCompletion(true))
		    {
		    	System.out.println( "TFIDF Completed Successfully ");
		    }
		    else
		    {
		    	System.out.println("TFIDF Job Failed");
		    	return ;
		    }
		    
		    //Similarity Matrix Job Phase I
		    Job similarityMatrix = new Job(conf, "Similarity Matrix I");
		    similarityMatrix.setJarByClass(OfflineRunner.class);
		    similarityMatrix.setMapperClass(SimilarityMatrixMapperPhaseI.class);
		    similarityMatrix.setReducerClass(SimilarityMatrixReducerPhaseI.class);
		    similarityMatrix.setOutputKeyClass(Text.class);
		    similarityMatrix.setOutputValueClass(Text.class);
		    
		    //Make output of previous job as input of this job
		    FileInputFormat.addInputPath(similarityMatrix, new Path(temporaryOutputPath));
		    
		    temporaryOutputPath = args[1] + "/similaritymatrixphaseI" ;
		    FileOutputFormat.setOutputPath(similarityMatrix, new Path( temporaryOutputPath ));
		    
		    //Starting job and wait for its completion
		    if(similarityMatrix.waitForCompletion(true))
		    {
		    	System.out.println( "Similarity Matrix Phase I Successfully ");
		    }
		    else
		    {
		    	System.out.println("Similarity Matrix Phase I Job Failed");
		    	return ;
		    }
		    
		    
		  //Similarity Matrix Job Phase II
		    Job similarityMatrix2 = new Job(conf, "Similarity Matrix II");
		    similarityMatrix2.setJarByClass(OfflineRunner.class);
		    similarityMatrix2.setMapperClass(SimilarityMatrixMapperPhaseII.class);
		    similarityMatrix2.setReducerClass(SimilarityMatrixReducerPhaseII.class);
		    similarityMatrix2.setOutputKeyClass(Text.class);
		    similarityMatrix2.setOutputValueClass(Text.class);
		    
		    //Make output of previous job as input of this job
		    FileInputFormat.addInputPath(similarityMatrix2, new Path(temporaryOutputPath));
		    temporaryOutputPath = args[1]+"/similaritymatrixfinal" ;
		    FileOutputFormat.setOutputPath(similarityMatrix2, new Path( temporaryOutputPath ));
		    
		    //Starting job and wait for its completion
		    if(similarityMatrix2.waitForCompletion(true))
		    {
		    	System.out.println( "Similarity Matrix Phase I Successfully ");
		    }
		    else
		    {
		    	System.out.println("Similarity Matrix Phase I Job Failed");
		    	return ;
		    }
		    if(!FileUtil.copyMerge(fs,new Path(temporaryOutputPath), FileSystem.getLocal(conf), new Path(args[2]),false, conf, null) )
		    {
		    	System.out.println("See the output on hdfs");
		    	return ;
		    }
		    fs.close();
		    
		    System.out.println("Output of all job is at "+args[2]) ;
		}
		catch ( Exception e ) 
		{
			e.printStackTrace() ;
			System.out.println(e.getMessage()) ;
		}
	}
}
