package assign2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;

public class Driver {
	
	int numberOfBooksToDisplay ;
	HashMap < String ,ArrayList<SimpleImmutableEntry <String,Double> >>  bookData = new HashMap <String , ArrayList<SimpleImmutableEntry<String,Double> >> () ;
	
	Driver (String [] args ) throws IOException
	{
		numberOfBooksToDisplay = 30 ;
		prepareHashMap ( args[0] ) ;
	}
	void prepareHashMap (String input ) throws IOException
	{
		System.out.println("...................PREPARING THE DB IN MEMORY...................");
		BufferedReader reader = new BufferedReader (new InputStreamReader ( new FileInputStream(input))) ;
		String temp = reader.readLine() ;
		
		while(temp != null )
		{
			String tokens [] = temp.split("\t");
			String book1 = tokens[0].split(":")[0] ;
			String book2 = tokens[0].split(":")[1] ;
			if ( !bookData.containsKey(book1) )
			{
				
				bookData.put(book1, new ArrayList <SimpleImmutableEntry <String , Double >> ()) ;
			}

			SimpleImmutableEntry <String , Double > simpleEntry = new SimpleImmutableEntry < String , Double > (book2,Double.parseDouble(tokens[1])) ;
			bookData.get(book1).add(simpleEntry) ;
			temp = reader.readLine() ;
		}
		reader.close();
		System.out.println("...................COMPLETED LOADING DB IN MEMORY...................");
		sortArrayList () ;
	}
	void sortArrayList ()
	{

		System.out.println("...................SORTING DATA IN MEMORY FOR QUICK ACCESS......................");
		for(String key : bookData.keySet() )
		{
			ArrayList <SimpleImmutableEntry <String , Double >> arrayList = bookData.get(key) ;
			Collections.sort(arrayList,new Comparator<SimpleImmutableEntry <String , Double >> ()
					{

						@Override
						public int compare(
								SimpleImmutableEntry<String, Double> o1,
								SimpleImmutableEntry<String, Double> o2) {
								return o1.getValue().compareTo(o2.getValue()) ;
						}	}
			);
		}
		System.out.println("...................SORTING DONE IN MEMORY......................") ;
		System.out.println("...................READY FOR RECOMMENDATION......................") ;
		
	}
	ArrayList <String>  getMatchedBooks (String bookname )
	{
		bookname = bookname.toLowerCase().replace(' ', '_') ;
		ArrayList<String> books = new ArrayList < String >(30) ;
		if ( bookData.containsKey(bookname) ) 
		{
			ArrayList <SimpleImmutableEntry <String , Double >> arrayList = bookData.get(bookname) ;
			for (int i = 0 ; i < arrayList.size() && i < numberOfBooksToDisplay ; i++ ) 
			{
				books.add( "("+ (i+1)+") " + arrayList.get(i).getKey().replace('_', ' ') );
			}
			if( arrayList.size() != numberOfBooksToDisplay )
			{
				books.add("There were only " + arrayList.size() +" books in db so cannot display "+ numberOfBooksToDisplay+" books.");
			}
		}
		else
		{
			books.add("Your Entered Book Doest not Exist in DB");
		}
		
		return books ;
	}
	public static void main ( String args []  )
	{
		
		if ( args.length != 1 )
		{
			System.out.println("Your aurguments should be like inputfilepath");
			return ;
		}
		try
		{
				Driver recommender = new Driver (args) ;
				System.out.println("Enter book name to find or type exit to quit program") ;
				String instruction ; 
				Scanner sc = new Scanner (System.in) ;
				instruction = sc.nextLine();
				while(!instruction.equals("exit"))
				{
					ArrayList<String> result = recommender.getMatchedBooks(instruction);
					if(result.size() == 0 )
					{
						System.out.println("The book not found in db or no matched books");
					}
					else
					{
						System.out.println("Following are the related books :");
						for (int i = 0 ; i < result.size() ; i++ )
						{
							System.out.println(result.get(i)) ;
						}
					}
					System.out.println("Enter book name to find or type exit to quit program") ;
					instruction = sc.nextLine() ;
				}
				sc.close();
				System.out.println("Exiting program");
		}
		catch (Exception e )
		{
			e.printStackTrace();
			System.out.println("Exception occured closing program") ;
		}
		
	}
}
