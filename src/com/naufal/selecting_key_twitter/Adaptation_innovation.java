package com.naufal.selecting_key_twitter;

import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * @author @Ahmadluky
 * chain mapreduce
 * tweet //
 * retweet //
 * 
 */
public class Adaptation_innovation {
	//tweet |akun, is_tweet|
	public static class TaskMapper_tweet extends Mapper<Object,Text,Text,IntWritable> {
		private Text akun = new Text();
    	private IntWritable tweet = new IntWritable(1);
    	private IntWritable tweet_zero = new IntWritable(0);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] split = value.toString().split(";");
			String is_tweet = split[1].toString();
        	if (is_tweet.equalsIgnoreCase("TRUE")) {
        		akun.set(split[0]);
    			context.write(akun, tweet);
			}else{
				akun.set(split[0]);
    			context.write(akun, tweet_zero); //|akun, 1/0|
			}
		}
	}
    public static class TaskReducer_tweet extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    //retweet |is_retweet_from|
	public static class TaskMapper_retweet extends Mapper<Object,Text,Text,IntWritable> {
	  	private Text akun = new Text();
    	private IntWritable retweet = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			if (!value.equals(null)) 
			{
				akun.set(value);
    			context.write(akun, retweet); //|akun, 1|
			}
		}
	}
    public static class TaskReducer_retweet extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    //tweet
    public static class TaskMapper_tweet2 extends Mapper<Object,Text,Text,Text> {
		private String fileTag = "TW~";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] split = value.toString().split("\t");
			String akun = split[0];
            context.write(new Text(akun), new Text(fileTag + split[1])); // |akun, tweet|
		}
	}
    //retweet
    public static class TaskMapper_retweet2 extends Mapper<Object,Text,Text,Text> {
		private String fileTag = "RTW~";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] split = value.toString().split("\t");
			String akun = split[0];
            context.write(new Text(akun), new Text(fileTag + split[1])); // |akun, retweet|
		}
	}
	//Favs |akun, favs|
	public static class TaskMapper_Favs extends Mapper<Object,Text,Text,Text> {
		private String fileTag = "FAVS~";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] split2 = value.toString().split(";");
			String akun = split2[0];
            context.write(new Text(akun), new Text(fileTag + split2[1])); // |akun, favs|
		}
	}
	public static class Red extends Reducer<Text,Text,Text,DoubleWritable> {
	  	private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
        	double inv = 0.0;
        	int favs=0;
        	int rtw=0;
        	int tw=0;
    
            for (Text val : values) 
            {
            	System.out.println(key.toString()+"/"+val.toString());
            	
                String valueSplitted[] = val.toString().split("~");
                
                if(valueSplitted[0].equals("FAVS"))
                	favs = Integer.parseInt( valueSplitted[1].trim() );
                
                if(valueSplitted[0].equals("RTW"))
                	rtw = Integer.parseInt( valueSplitted[1].trim() );
                else
                	rtw = 0;
                
                if(valueSplitted[0].equals("TW"))
                	tw = Integer.parseInt( valueSplitted[1].trim() );
                
                if (tw==0)
	                inv = favs;
	            else 
	            	inv = favs + (rtw/tw); // Adaptation inovation
            }
            
            result.set(inv);
            context.write(key, result);
        }
	}
    private static void usage() {
        System.err.println("usage: hadoop -jar <...> Adaptation_innovation <input tweet> <output tweet> <input retweet> <output retweet> <input favs> <output result>");
//      data-in/tweet/data.txt data-out/tweet data-in/retweet/data.txt data-out/retweet data-in/favs/data.txt data-out/result
//      data-in/adaptation_innovation/tweet/data.txt data-out/adaptation_innovation/tweet 
//      data-in/adaptation_innovation/retweet/data.txt data-out/adaptation_innovation/retweet 
//      data-in/adaptation_innovation/favs/data.txt 
//      data-out/adaptation_innovation/result
        System.exit(-1);
    }
	public static void main(String[] args) throws Exception
	{
		if (args.length != 6)
        usage();

        System.out.println("Starting Job");
        long startTime = new Date().getTime();
        
		Configuration c=new Configuration();
		String[] dir=new GenericOptionsParser(c,args).getRemainingArgs();
		String IN_TWEET=dir[0]; //tweet path
		String OUT_TWEET=dir[1]; //tweet out path
		/**
    	 * Remove directory output if existing
    	 **/
    	FileSystem fs_tweet = FileSystem.get(c);
    	if (fs_tweet.exists(new Path(OUT_TWEET)))
        	fs_tweet.delete(new Path(OUT_TWEET), true);

		Path p_tweet=new Path(IN_TWEET);
		Path p_tweet2=new Path(OUT_TWEET);
		Job j = Job.getInstance(c,"tweet");
		j.setJarByClass(Adaptation_innovation.class);
		j.setMapperClass(TaskMapper_tweet.class);
		j.setReducerClass(TaskReducer_tweet.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, p_tweet);
		FileOutputFormat.setOutputPath(j, p_tweet2);
		j.waitForCompletion(true);     
        if (!j.isSuccessful()) {
        	System.out.println("Job Finding Tweet " + j.getJobID() + " failed!");
        	System.exit(1);
        }
		
		String IN_RETWEET = dir[2];//retweet path
		String OUT_RETWEET = dir[3];// retweet output path
		Configuration c3=new Configuration();
		/**
    	 * Remove directory output if existing
    	 **/
    	FileSystem fs_tweet3 = FileSystem.get(c3);
    	if (fs_tweet3.exists(new Path(OUT_RETWEET))) 
        	fs_tweet3.delete(new Path(OUT_RETWEET), true);
		
		Path p_retweet=new Path(IN_RETWEET); 
		Path p_retweet2=new Path(OUT_RETWEET); 
		Job j3 = Job.getInstance(c3,"retweet");
		j3.setJarByClass(Adaptation_innovation.class);
		j3.setMapperClass(TaskMapper_retweet.class);
		j3.setReducerClass(TaskReducer_retweet.class);
		j3.setOutputKeyClass(Text.class);
		j3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j3, p_retweet);
		FileOutputFormat.setOutputPath(j3, p_retweet2);
		j3.waitForCompletion(true);     
        if (!j3.isSuccessful()) {
        	System.out.println("Job Finding ReTweet " + j3.getJobID() + " failed!");
        	System.exit(1);
        }
        
		String IN_FAVS = dir[4];//favs path
		String OUT = dir[5]; // result output path
		Configuration c2=new Configuration();
		/**
    	 * Remove directory output if existing
    	 **/
    	FileSystem fs_tweet2 = FileSystem.get(c2);
    	if (fs_tweet2.exists(new Path(OUT))) 
        	fs_tweet2.delete(new Path(OUT), true);
		
		Path p1=new Path(OUT_TWEET); 
		Path p2=new Path(OUT_RETWEET);
		Path p3=new Path(IN_FAVS);
		Path p4=new Path(OUT);
		Job j2 = Job.getInstance(c2,"multiple");
		j2.setJarByClass(Adaptation_innovation.class);
		j2.setMapperClass(TaskMapper_tweet2.class);
		j2.setMapperClass(TaskMapper_retweet2.class);
		j2.setMapperClass(TaskMapper_Favs.class);
		j2.setReducerClass(Red.class);
		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(j2, p1, TextInputFormat.class, TaskMapper_tweet2.class);
		MultipleInputs.addInputPath(j2, p2, TextInputFormat.class, TaskMapper_retweet2.class);
		MultipleInputs.addInputPath(j2, p3, TextInputFormat.class, TaskMapper_Favs.class);
		FileOutputFormat.setOutputPath(j2, p4);
		j2.waitForCompletion(true);     
        if (!j2.isSuccessful()) {
        	System.out.println("Job Finding Adaptation " + j2.getJobID() + " failed!");
        	System.exit(1);
        }        
        final double duration = (System.currentTimeMillis() - startTime)/1000.0;
        System.out.println("Job Finished in " + duration + " seconds");
	}
}
