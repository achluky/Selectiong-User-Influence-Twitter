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
import org.apache.hadoop.util.GenericOptionsParser;
//import org.apache.log4j.PropertyConfigurator;

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

	public static String MENTION_IN = "data-in/accessibility/mention/data.txt";
	public static String MENTION_OUT = "data-out/accessibility/mention/";
	public static String REPLY_IN = "data-in/accessibility/reply/data.txt";
	public static String REPLY_OUT = "data-out/accessibility/reply/";
	public static String RETWEET_IN = "data-in/accessibility/retweet/data.txt";
	public static String RETWEET_OUT = "data-out/accessibility/retweet/";
	public static String LIKE_IN = "data-in/accessibility/favs/data.txt";
	public static String FOLLOWING_IN = "data-in/accessibility/following/data.txt";
	public static String RESULT_OUT = "data-out/accessibility/result/";
	
	//memention |akun|mention|
    public static class TaskMapper_mention extends Mapper<Object, Text, Text, IntWritable>{
    	private Text akun = new Text();
    	private IntWritable mention = new IntWritable();
    	private IntWritable mention_zero = new IntWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] split = value.toString().split(";");
        	if (split.length>1) {
				akun.set(split[0]);
				String[] m = split[1].split(",");
				Integer x = m.length;
				mention.set(x);
    			context.write(akun, mention);
			}else{
    			context.write(akun, mention_zero);
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
    // like accessibility |akun, like|
    public static class TaskMapper_accessibilityLike extends Mapper<Object, Text, Text, Text>{
    	private Text word = new Text();
		private String fileTag = "~LIKE";
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
        	Double like = 0.0;
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
	           	if (split[1].equals("LIKE")) {
	           		like = Double.parseDouble(split[0]);
	           	}  
            }
           	rst = following + reply + mention + meretweet + like;
            result.set(rst);
            context.write(key, result);
        }
    }
    // ========================================================================
    // nilai_aksesibilitas = following + reply + retweet + like + mention tweet
    // ========================================================================
    // 1. get akun dan following
    // 1. get jml_reply berdasarkan akun
    // 2. get retweet yang dilakukan akun berdasarkan akun
    // 3. get jumlah like yang dikakukan akun
    // 5. get jumlah mention akun dari akun lain
    // ========================================================================
    private static void usage() {
        System.err.println("usage: hadoop -jar <...> Accessibility "
        							+ "<input mention> <output mention> "
        							+ "<input reply> <output reply> "
        							+ "<input retweet> <output retweet> "
        							+ "<input like> "
        							+ "<input following> "
        							+ "<output result>");
        System.exit(-1);
    }
    @SuppressWarnings("unused")
	public static void main(String[] args) throws Exception {
    	
    	System.out.println("Starting Job Accesibility");
		long startTime = new Date().getTime();
		
        String IN_MENTION;
		String OUT_MENTION;
		String IN_REPLY;
		String OUT_REPLY;
		String IN_RETWEET;
		String OUT_RETWEET;
		String IN_LIKE;
		String IN_FOLLOWING;
		String OUT_RESULT;
        if (false) {
			if (args.length == 9){
				Configuration c=new Configuration();
				String[] dir=new GenericOptionsParser(c,args).getRemainingArgs();
				IN_MENTION=dir[0];
				OUT_MENTION=dir[1];
				IN_REPLY=dir[2];
				OUT_REPLY=dir[3];
				IN_RETWEET=dir[4];
				OUT_RETWEET=dir[5];
				IN_LIKE=dir[6];
				IN_FOLLOWING=dir[7];
				OUT_RESULT=dir[8];
		    	FileSystem fs = FileSystem.get(c);
		    	if (fs.exists(new Path(OUT_RESULT)))
		    	{
		    		fs.delete(new Path(OUT_MENTION), true);
		    		fs.delete(new Path(OUT_REPLY), true);
		    		fs.delete(new Path(OUT_RETWEET), true);
		    		fs.delete(new Path(OUT_RESULT), true);
		    	}
			}else{
				usage();
			}
		}else{

//			String log4jConfPath = "C:\\Users\\DIDSI-IPB\\workspace\\Selecting-keyFaceboo\\log4j.properties";
//			PropertyConfigurator.configure(log4jConfPath);
//			
			IN_MENTION=MENTION_IN;
			OUT_MENTION=MENTION_OUT;
			IN_REPLY=REPLY_IN;
			OUT_REPLY=REPLY_OUT;
			IN_RETWEET=RETWEET_IN;
			OUT_RETWEET=RETWEET_OUT;
			IN_LIKE=LIKE_IN;
			IN_FOLLOWING=FOLLOWING_IN;
			OUT_RESULT=RESULT_OUT;

	    	Configuration conf = new Configuration();
	    	FileSystem fs = FileSystem.get(conf);
	    	if (fs.exists(new Path(OUT_MENTION))) {
	    		fs.delete(new Path(OUT_MENTION), true);
	    		fs.delete(new Path(OUT_REPLY), true);
	    		fs.delete(new Path(OUT_RETWEET), true);
	    		fs.delete(new Path(OUT_RESULT), true);
			}
	    	
	    	/** GET MENTION **/
	        Job job_mention = Job.getInstance(conf, "Mention_Count");
	        job_mention.setJarByClass(Accessibility.class);
	        job_mention.setMapperClass(TaskMapper_mention.class);
	        job_mention.setCombinerClass(IntSumReducer.class);
	        job_mention.setReducerClass(TaskReducer_mention.class);
	        job_mention.setOutputKeyClass(Text.class);
	        job_mention.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job_mention, new Path(IN_MENTION));
	        FileOutputFormat.setOutputPath(job_mention, new Path(OUT_MENTION));
	        job_mention.waitForCompletion(true);     
	        if (!job_mention.isSuccessful()) {
	        	System.out.println("Job Mention Finding Local " + job_mention.getJobID() + " failed!");
	        	System.exit(1);
	        }
	        
	    	/** GET REPLY **/
	        Job job_reply = Job.getInstance(conf, "Reply_Count");
	        job_reply.setJarByClass(Accessibility.class);
	        job_reply.setMapperClass(TaskMapper_reply.class);
	        job_reply.setCombinerClass(IntSumReducer.class);
	        job_reply.setReducerClass(TaskReducer_reply.class);
	        job_reply.setOutputKeyClass(Text.class);
	        job_reply.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job_reply, new Path(IN_REPLY));
	        FileOutputFormat.setOutputPath(job_reply, new Path(OUT_REPLY));
	        job_reply.waitForCompletion(true);     
	        if (!job_reply.isSuccessful()) {
	          System.out.println("Job Reply Finding Local " + job_reply.getJobID() + " failed!");
	          System.exit(1);
	        }
	        
	        /** GET MERETWEET **/
	        Job job_meretweet = Job.getInstance(conf, "Meretweet_Count");
	        job_meretweet.setJarByClass(Accessibility.class);
	        job_meretweet.setMapperClass(TaskMapper_meretweet.class);
	        job_meretweet.setCombinerClass(IntSumReducer.class);
	        job_meretweet.setReducerClass(TaskReducer_meretweet.class);
	        job_meretweet.setOutputKeyClass(Text.class);
	        job_meretweet.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job_meretweet, new Path(IN_RETWEET));
	        FileOutputFormat.setOutputPath(job_meretweet, new Path(OUT_RETWEET));
	        job_meretweet.waitForCompletion(true);     
	        if (!job_meretweet.isSuccessful()) {
			  System.out.println("Job Meretweet Finding Local " + job_meretweet.getJobID() + " failed!");
			  System.exit(1);
	        }
	        
	    	/**
	    	 * ACCESIBILITY
	    	 * Nilai Follower yang digunakan ada yang field pertama
	    	 * ====================================================
	    	 */
			Path p3=new Path(OUT_REPLY);
			Path p4=new Path(OUT_MENTION);
			Path p7=new Path(OUT_RETWEET);
			Path p5=new Path(IN_FOLLOWING);
			Path p8=new Path(IN_LIKE);
			Path p6=new Path(OUT_RESULT);
			
	        Job job=Job.getInstance(conf, "Accessibility_Calculation");
	        job.setJarByClass(Accessibility.class);
	        job.setMapperClass(TaskMapper_accessibilityReply.class);
	        job.setMapperClass(TaskMapper_accessibilityMeretweet.class);
	        job.setMapperClass(TaskMapper_accessibilityMention.class);
	        job.setMapperClass(TaskMapper_accessibilityFollowing.class);
	        job.setMapperClass(TaskMapper_accessibilityLike.class);
	        job.setReducerClass(TaskReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
			MultipleInputs.addInputPath(job, p3, TextInputFormat.class, TaskMapper_accessibilityReply.class);
			MultipleInputs.addInputPath(job, p7, TextInputFormat.class, TaskMapper_accessibilityMeretweet.class);
			MultipleInputs.addInputPath(job, p4, TextInputFormat.class, TaskMapper_accessibilityMention.class);
			MultipleInputs.addInputPath(job, p5, TextInputFormat.class, TaskMapper_accessibilityFollowing.class);
			MultipleInputs.addInputPath(job, p8, TextInputFormat.class, TaskMapper_accessibilityLike.class);
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
}

