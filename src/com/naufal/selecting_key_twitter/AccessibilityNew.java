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
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
//import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 
 * @author ahmadluky
 * Chain Job Mapreduce
 * formulasi : following + reply + retweet + like/favorited + mention tweet
 * ==
 * Warning !! : Dapatkan Jumlah Like setiap akun
 * 
 */
public class AccessibilityNew {
	// reply |akun, is_reply|
	public static class TaskMapper_reply extends Mapper<Object, Text, Text, IntWritable>{
    	private Text word = new Text();
    	private IntWritable reply = new IntWritable(1);
    	private IntWritable reply_zero = new IntWritable(0);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] split = value.toString().split(";");
        	if (split[1].equalsIgnoreCase("TRUE")) {
    			word.set(split[0]);
    			context.write(word, reply);
			}else{
    			word.set(split[0]);
    			context.write(word, reply_zero);
			}
        }
    }
    public static class TaskReducer_reply extends Reducer<Text,IntWritable,Text,IntWritable> {
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
    //memention |akun|mention|
    public static class TaskMapper_mention extends Mapper<Object, Text, Text, IntWritable>{
    	private Text word = new Text();
    	private IntWritable mention = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if (!value.equals(null)) {
				String tString = value.toString().replace('"', ' ');
				String tString2 = tString.replaceAll("\\s+","");
	        	String[] split = tString2.split(",");
	        	if ( split.length>0) {
	        		for (int i = 0; i < split.length; i++) {
	        			if (!split[i].equalsIgnoreCase(" ")) {
	        				word.set(split[i]);
			    			context.write(word, mention);
						}
					}
				}
			}
		}
    }
    public static class TaskReducer_mention extends Reducer<Text,IntWritable,Text,IntWritable> {
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
    //retweet : meretweet |retweet|TRUE/FALSE|
    public static class TaskMapper_meretweet extends Mapper<Object, Text, Text, IntWritable>{
    	private Text word = new Text();
    	private IntWritable meretweet = new IntWritable(1);
    	private IntWritable meretweet_zero = new IntWritable(0);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] split = value.toString().split(";");
        	if (split[1].equalsIgnoreCase("TRUE")) {
    			word.set(split[0]);
    			context.write(word, meretweet);
			}else{
    			word.set(split[0]);
    			context.write(word, meretweet_zero);
			}
        }
    }
    public static class TaskReducer_meretweet extends Reducer<Text,IntWritable,Text,IntWritable> {
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
    // accessibility ===========================================================================
    // reply accessibility 
    public static class TaskMapper_accessibilityReply extends Mapper<Object, Text, Text, Text>{
     	private Text word = new Text();
		private String fileTag = "~REPLY";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         	String[] split = value.toString().split("\t");
 			word.set(split[0]);
 			context.write(word, new Text(split[1]+fileTag));
        }
    }
    // meretweet accessibility 
    public static class TaskMapper_accessibilityMeretweet extends Mapper<Object, Text, Text, Text>{
     	private Text word = new Text();
		private String fileTag = "~MERETWEET";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         	String[] split = value.toString().split("\t");
 			word.set(split[0]);
 			context.write(word, new Text(split[1]+fileTag));
        }
    }
    // mention accessibility 
    public static class TaskMapper_accessibilityMention extends Mapper<Object, Text, Text, Text>{
    	private Text word = new Text();
		private String fileTag = "~MENTION";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         	String[] split = value.toString().split("\t");
 			word.set(split[0]);
 			context.write(word, new Text(split[1]+fileTag));
        }
    }
    // following accessibility |akun, following|
    public static class TaskMapper_accessibilityFollowing extends Mapper<Object, Text, Text, Text>{
    	private Text word = new Text();
		private String fileTag = "~FOLLOWING";
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         	String[] split = value.toString().split(";");
 			word.set(split[0]);
 			context.write(word, new Text(split[1]+fileTag));
        }
    }
    public static class TaskReducer extends Reducer<Text,Text,Text,DoubleWritable> {
    	private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	Double rst = 0.0;
        	Double mention = 0.0;
        	Double reply = 0.0;
        	Double following = 0.0;
        	Double meretweet = 0.0;
            for (Text val : values) {
	           	String[] split = val.toString().split("~");
	           	if (split[1].equals("MENTION")) {
	           		mention = Double.parseDouble(split[0]);
	           	}
	           	if (split[1].equals("REPLY")) {
	           		reply = Double.parseDouble(split[0]);
	           	}
	           	if (split[1].equals("FOLLOWING")) {
	           		following = Double.parseDouble(split[0]);
	           	}  
	           	if (split[1].equals("MERETWEET")) {
	           		meretweet = Double.parseDouble(split[0]);
	           	}  
            }
           	rst = following + reply + mention + meretweet;
            result.set(rst);
            context.write(key, result);
        }
    }
    // ========================================================================
    // nilai_aksesibilitas = following + reply + retweet + like + mention tweet
    // 1. get akun dan following
    // 1. get jml_reply berdasarkan akun
    // 2. get retweet yang dilakukan akun berdasarkan akun
    // 3. get jumlah like yang dikakukan akun
    // 5. get jumlah mention akun dari akun lain
    // ========================================================================
    //private static void usage() {
    	// data-in/accessibility/sentimen data-out/accessibility/sentimen 
    	// data-in/accessibility/quote data-out/accessibility/quote 
    	// data-in/accessibility/mention data-out/accessibility/mention 
    	// data-in/accessibility/reply data-out/accessibility/reply 
    	// data-in/accessibility/following data-out/accessibility/out
        //System.err.println("usage: hadoop -jar <...> Accessibility <input sentimen> <output sentiment> "
        //					+ "<input quote> <output quote> <input mention> <output mention>"
        //					+ "<input reply> <output reply> <input following> <output result>");
        //System.exit(-1);
    //}
    public static void main(String[] args) throws Exception {
    	//Configuration conf_sentiment = new Configuration();
		//String[] dir=new GenericOptionsParser(conf_sentiment,args).getRemainingArgs();
		
		//if (args.length != 10)
	    //    usage();

        System.out.println("Starting Job");
        long startTime = new Date().getTime();
    	/**
    	 * get mention
    	 */
        String MENTION_IN = "data-in/accessibility/mention/data.txt"; //String MENTION_IN = dir[4];
    	String MENTION_OUT = "data-out/accessibility/mention/data.txt"; //String MENTION_OUT = dir[5];
    	Configuration conf_mention = new Configuration();
    	FileSystem fs_mention = FileSystem.get(conf_mention);
    	if (fs_mention.exists(new Path(MENTION_OUT))) {
        	fs_mention.delete(new Path(MENTION_OUT), true);
		}
        Job job_mention = Job.getInstance(conf_mention, "Mention_count");
        job_mention.setJarByClass(Accessibility.class);
        job_mention.setMapperClass(TaskMapper_mention.class);
        job_mention.setCombinerClass(IntSumReducer.class);
        job_mention.setReducerClass(TaskReducer_mention.class);
        job_mention.setOutputKeyClass(Text.class);
        job_mention.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_mention, new Path(MENTION_IN));
        FileOutputFormat.setOutputPath(job_mention, new Path(MENTION_OUT));
        job_mention.waitForCompletion(true);     
        if (!job_mention.isSuccessful()) {
        	System.out.println("Job Mention Finding Local " + job_mention.getJobID() + " failed!");
        	System.exit(1);
        }
    	/**
    	 * get jumlah_reply
    	 */
        String JUMLAH_REPLY_IN = "data-in/accessibility/reply/data.txt"; //String JUMLAH_REPLY_IN = dir[6];
        String JUMLAH_REPLY_OUT = "data-out/accessibility/reply/data.txt"; //String JUMLAH_REPLY_OUT = dir[7];
    	Configuration conf_reply = new Configuration();
    	FileSystem fs_reply = FileSystem.get(conf_reply);
    	if (fs_reply.exists(new Path(JUMLAH_REPLY_OUT))) {
        	fs_reply.delete(new Path(JUMLAH_REPLY_OUT), true);
		}
        Job job_reply = Job.getInstance(conf_reply, "Reply_count");
        job_reply.setJarByClass(Accessibility.class);
        job_reply.setMapperClass(TaskMapper_reply.class);
        job_reply.setCombinerClass(IntSumReducer.class);
        job_reply.setReducerClass(TaskReducer_reply.class);
        job_reply.setOutputKeyClass(Text.class);
        job_reply.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_reply, new Path(JUMLAH_REPLY_IN));
        FileOutputFormat.setOutputPath(job_reply, new Path(JUMLAH_REPLY_OUT));
        job_reply.waitForCompletion(true);     
        if (!job_reply.isSuccessful()) {
          System.out.println("Job Reply Finding Local " + job_reply.getJobID() + " failed!");
          System.exit(1);
        }
        /**
    	 * get jumlah meretweet
    	 */
        String JUMLAH_MERETWEET_IN = "data-in/accessibility/retweet/data.txt"; //String JUMLAH_REPLY_IN = dir[6];
        String JUMLAH_MERETWEET_OUT = "data-out/accessibility/retweet/data.txt"; //String JUMLAH_REPLY_OUT = dir[7];
    	Configuration conf_meretweet = new Configuration();
    	FileSystem fs_meretweet = FileSystem.get(conf_meretweet);
    	if (fs_meretweet.exists(new Path(JUMLAH_MERETWEET_OUT))) {
    		fs_meretweet.delete(new Path(JUMLAH_MERETWEET_OUT), true);
		}
        Job job_meretweet = Job.getInstance(conf_reply, "meretweet_count");
        job_meretweet.setJarByClass(Accessibility.class);
        job_meretweet.setMapperClass(TaskMapper_meretweet.class);
        job_meretweet.setCombinerClass(IntSumReducer.class);
        job_meretweet.setReducerClass(TaskReducer_meretweet.class);
        job_meretweet.setOutputKeyClass(Text.class);
        job_meretweet.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_meretweet, new Path(JUMLAH_MERETWEET_IN));
        FileOutputFormat.setOutputPath(job_meretweet, new Path(JUMLAH_MERETWEET_OUT));
        job_meretweet.waitForCompletion(true);     
        if (!job_meretweet.isSuccessful()) {
		  System.out.println("Job Meretweet Finding Local " + job_meretweet.getJobID() + " failed!");
		  System.exit(1);
        }
    	/**
    	 * Accessibility
    	 * Nilai Follower yang digunakan ada yang field pertama
    	 * ====================================================
    	 */
        String FOLLOWING = "data-in/accessibility/following/data.txt"; //String FOLLOWING = dir[8];
    	String RESULT = "data-out/accessibility/result"; //String RESULT = dir[9];
    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(conf);
    	if (fs.exists(new Path(RESULT))) {
        	fs.delete(new Path(RESULT), true);
		}
		Path p3=new Path(JUMLAH_REPLY_OUT);
		Path p4=new Path(MENTION_OUT);
		Path p7=new Path(JUMLAH_MERETWEET_OUT);
		Path p5=new Path(FOLLOWING);
		Path p6=new Path(RESULT);
        Job job=Job.getInstance(conf, "Accessibility Calculation");
        job.setJarByClass(Accessibility.class);
        job.setMapperClass(TaskMapper_accessibilityReply.class);
        job.setMapperClass(TaskMapper_accessibilityMeretweet.class);
        job.setMapperClass(TaskMapper_accessibilityMention.class);
        job.setMapperClass(TaskMapper_accessibilityFollowing.class);
        job.setReducerClass(TaskReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, p3, TextInputFormat.class, TaskMapper_accessibilityReply.class);
		MultipleInputs.addInputPath(job, p7, TextInputFormat.class, TaskMapper_accessibilityMeretweet.class);
		MultipleInputs.addInputPath(job, p4, TextInputFormat.class, TaskMapper_accessibilityMention.class);
		MultipleInputs.addInputPath(job, p5, TextInputFormat.class, TaskMapper_accessibilityFollowing.class);
        FileOutputFormat.setOutputPath(job, p6);
        job.waitForCompletion(true);     
        if (!job.isSuccessful()) {
          System.out.println("Job Finding Local " + job.getJobID() + " failed!");
          System.exit(1);
        }
        final double duration = (System.currentTimeMillis() - startTime)/1000.0;
        System.out.println("Job Finished in " + duration + " seconds");
    }
}
