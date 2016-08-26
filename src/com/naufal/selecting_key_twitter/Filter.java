package com.naufal.selecting_key_twitter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
//import com.naufal.util.io.FileUtils;

/**
 * 
 * @author ahmadluky
 * komunikasi eksternal = popularity + tff
 */
public class Filter {
	public static String A = "./data-out/accessibility/result/part-r-00000";
	public static String B = "./data-out/comunication_exsternal/part-r-00000";
	
    public static Multimap<String, String> myMultimapPopulatiry = ArrayListMultimap.create();
    public static Multimap<String, String> myMultimapTff = ArrayListMultimap.create();
    public static Multimap<String, String> rstPopularity;
	public static  Multimap<String, String> rstTff;
	
	public static Multimap<String, String> readPopularity(Multimap<String, String> myMultimapP) throws FileNotFoundException, IOException{
		try(BufferedReader br = new BufferedReader(new FileReader(A))) {
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
		        String[] s= sCurrentLine.split(",");
		 	    myMultimapP.put(s[0], s[1]);
			}
		    return myMultimapP;
		}
	}
	public static void popularity() throws FileNotFoundException, IOException{
		rstPopularity = readPopularity(myMultimapPopulatiry);
	}
	
	public static Multimap<String, String> readTff(Multimap<String, String> myMultimapTff2) throws FileNotFoundException, IOException{
		try(BufferedReader br = new BufferedReader(new FileReader(B))) {
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				//calculation popularity Nagmoti
		        String[] s= sCurrentLine.split(",");
		        myMultimapTff2.put(s[0], s[1]);
			}
		    return myMultimapTff2;
		}
	}
	public static void tff() throws IOException{
		rstTff = readTff(myMultimapTff);
	}
	
	public static void main(String[] args) throws IOException{
        System.out.println("Starting Job Exsternal Comunication");
        long startTime = new Date().getTime();
        
		// - praprosesing -
		popularity();
		tff();
		
		// - claculation -
		for(String key : rstPopularity.keySet()) {
			Collection<String> valuePopularity = myMultimapPopulatiry.get(key);
			Collection<String> valueTff = myMultimapTff.get(key);
	        System.out.println(key+"\t"+valuePopularity.toString()+"\t"+valueTff.toString());
			//FileUtils.writefile(""+key+"\t"+co_exs+"", FILE_FOLLOWER_FOLLOWING_OUT);
		}
		
        final double duration = (System.currentTimeMillis() - startTime)/1000.0;
        System.out.println("Job Exsternal Comunication Finished in " + duration + " seconds");
	}
}
