package com.naufal.selecting_key_twitter;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.log4j.PropertyConfigurator;

/**
 * exsternal communication
 * @author ahmadluky
 */
public class Exsternal_communication {
		public static String FILE_FOLLOWER_FOLLOWING_IN = "./data-in/comunication_exsternal/follower-following-unique.txt";
		public static String FILE_FOLLOWER_FOLLOWING_OUT = "./data-out/comunication_exsternal/";
		
		// |akun, follower, following|
		public static class TaskMapper_eksternalCominication extends Mapper<Object,Text,Text,DoubleWritable> {
	    	private DoubleWritable co_exs = new DoubleWritable();
	    	private Text akun = new Text();
			public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
			{
				String[] split = value.toString().split(",");
				akun.set(split[0].toString());
				//popularity
				Integer d = Integer.parseInt(split[1])+Integer.parseInt(split[2]);
		        Double popularityValue;
		        if (d==0) {
			        popularityValue = 0.0;
				}else{
			        popularityValue =  Double.parseDouble(split[1])/d.doubleValue();
				}
		        // rff_r
		        Double tff_r;
		        if (Integer.parseInt(split[2])==0) {
		        	tff_r=0.0;
				}else{
					tff_r=Double.parseDouble(split[1])/Double.parseDouble(split[2]);
				}
		        // exsternal comunication
		        if (tff_r==0.0) {
					co_exs.set(0.0);
				}else{
					co_exs.set((popularityValue+ tff_r)/2);
				}
	    		context.write(akun, co_exs); //|akun, popularity|
			}
		}
	    public static class TaskReducer_eksternalCominication extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	    	public void reduce(Text key, DoubleWritable values, Context context) throws IOException, InterruptedException {
	    		context.write(key, values);
	        }
	    }
	    private static void usage() {
	        System.err.println("usage: hadoop -jar <...> Exsternal_communication <input tweet> <output tweet>");
	        System.exit(-1);
	    }
	    /**
	     * ==================================================
	     * @ahmadluky
	     * ==================================================
	     */
		@SuppressWarnings("unused")
		public static void main(String[] args) throws Exception
		{
			//String log4jConfPath = "C:\\Users\\DIDSI-IPB\\workspace\\Selecting-keyFaceboo\\log4j.properties";
			//PropertyConfigurator.configure(log4jConfPath);
			
	        System.out.println("Starting Job Exsternal_communication");
			long startTime = new Date().getTime();
	        
	        String IN;
			String OUT;
	        if (false) {
				if (args.length == 2){
					Configuration c=new Configuration();
					String[] dir=new GenericOptionsParser(c,args).getRemainingArgs();
					IN=dir[0];
					OUT=dir[1];
			    	FileSystem fs = FileSystem.get(c);
			    	if (fs.exists(new Path(OUT)))
			    		fs.delete(new Path(OUT), true);
				}else{
					usage();
				}
			}else{
		        IN=FILE_FOLLOWER_FOLLOWING_IN;
				OUT=FILE_FOLLOWER_FOLLOWING_OUT;
				Configuration c=new Configuration();
		    	FileSystem fs_tweet = FileSystem.get(c);
		    	if (fs_tweet.exists(new Path(OUT)))
		        	fs_tweet.delete(new Path(OUT), true);
				Path in=new Path(IN);
				Path out=new Path(OUT);
				Job j = Job.getInstance(c,"Exsternal_communication");
				j.setJarByClass(Adaptation_innovation.class);
				j.setMapperClass(TaskMapper_eksternalCominication.class);
				j.setReducerClass(TaskReducer_eksternalCominication.class);
				j.setOutputKeyClass(Text.class);
				j.setOutputValueClass(DoubleWritable.class);
		        FileInputFormat.addInputPath(j, in);
				FileOutputFormat.setOutputPath(j, out);
				j.waitForCompletion(true);     
		        if (!j.isSuccessful()) {
		        	System.out.println("Job Finding Exsternal_communication " + j.getJobID() + " failed!");
		        	System.exit(1);
		        }
		        final double duration = (System.currentTimeMillis() - startTime)/1000.0;
		        System.out.println("Job Exsternal Comunication Finished in " + duration + " seconds");
			}
		}
}
