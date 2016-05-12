package com.naufal.selecting_key_twitter;

import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

/**
 * Adaptasi Inovasi 
 * = favorited * (pos. count/ neg.count)
 * @author ahmadluky
 *
 */
public class Adaptation_innovation_old {
	//tweet |akun, is_tweet|
    public static class TaskMapper_tweet extends Mapper<Object, Text, Text, IntWritable>{
    	private Text word = new Text();
    	private IntWritable retweet = new IntWritable(1);
    	private IntWritable retweet_zero = new IntWritable(0);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split(",");
        	if (split[1].equalsIgnoreCase("TRUE")) {
    			word.set(split[0]);
    			context.write(word, retweet);
			}else{
    			word.set(split[0]);
    			context.write(word, retweet_zero);
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
    //retweet |is_retweet_freom|
    public static class TaskMapper_retweet extends Mapper<Object, Text, Text, IntWritable>{
    	private Text word = new Text();
    	private IntWritable retweet = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			if (!value.equals(null)) {
    			word.set(value);
    			context.write(word, retweet);
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
    // adaptasi inovasi
	public static class TaskMapper extends Mapper<Object, Text, Text, IntWritable>{
    	private Text word = new Text();
    	private IntWritable rst_temp = new IntWritable();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] split = value.toString().split(";");
			word.set(split[0]);
			int sum = 0;
			if (split.length == 4) {
				if (Integer.parseInt(split[2])==0) {
					rst_temp.set(0);
				} else {
					sum += Integer.parseInt(split[3]) * (Integer.parseInt(split[1]) / Integer.parseInt(split[2]));
				}
				rst_temp.set(sum);
				context.write(word, rst_temp);
			}
        }
    }
    public static class TaskReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
    public static void main(String[] args) throws Exception {
    	/**
    	 * get tweet ================= |akun, is_tweet|
    	 */
    	String IN_TWEET = "data-in/accessibility/tweet";
    	String IN_RETWEET = "data-in/accessibility/retweet";
    	String OUT_TWEET = "data-out/accessibility/tweet";
    	String OUT_RETWEET = "data-out/accessibility/retweet";
    	
    	Configuration conf_tweet = new Configuration();
    	/**
    	 * Remove directory output if existing
    	 **/
    	FileSystem fs_tweet = FileSystem.get(conf_tweet);
    	if (fs_tweet.exists(new Path(OUT_TWEET))) {
        	fs_tweet.delete(new Path(OUT_TWEET), true);
		}
        Job job_tweet = Job.getInstance(conf_tweet, "Reply_count");
        job_tweet.setJarByClass(Accessibility.class);
        job_tweet.setMapperClass(TaskMapper_tweet.class);
        /**
         * Combiner 
         **/
        job_tweet.setCombinerClass(IntSumReducer.class);
        job_tweet.setReducerClass(TaskReducer_tweet.class);
        job_tweet.setOutputKeyClass(Text.class);
        job_tweet.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_tweet, new Path(IN_TWEET));
        FileOutputFormat.setOutputPath(job_tweet, new Path(OUT_TWEET));
        /**
         * Runing Time hadoop JOB
         */
        System.out.println("Starting Job");
        long startTime = new Date().getTime();
        job_tweet.waitForCompletion(true);     
        if (!job_tweet.isSuccessful()) {
          System.out.println("Job Finding Tweet " + job_tweet.getJobID() + " failed!");
          System.exit(1);
        }
        
    	/**
    	 * get retweet ================= |is_retwee_from|
    	 */
    	Configuration conf_retweet = new Configuration();
    	/**
    	 * Remove directory output if existing
    	 **/
    	FileSystem fs_retweet = FileSystem.get(conf_retweet);
    	if (fs_retweet.exists(new Path(OUT_RETWEET))) {
        	fs_retweet.delete(new Path(OUT_RETWEET), true);
		}
        Job job_retweet = Job.getInstance(conf_retweet, "Reply_count");
        job_retweet.setJarByClass(Accessibility.class);
        job_retweet.setMapperClass(TaskMapper_retweet.class);
        /**
         * Combiner 
         **/
        job_retweet.setCombinerClass(IntSumReducer.class);
        job_retweet.setReducerClass(TaskReducer_retweet.class);
        job_retweet.setOutputKeyClass(Text.class);
        job_retweet.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_retweet, new Path(IN_RETWEET));
        FileOutputFormat.setOutputPath(job_retweet, new Path(OUT_RETWEET));
        /**
         * Runing Time hadoop JOB
         */
        job_retweet.waitForCompletion(true);     
        if (!job_retweet.isSuccessful()) {
          System.out.println("Job Finding retweet " + job_retweet.getJobID() + " failed!");
          System.exit(1);
        }
        
        /**
    	 * adaptasi inovasi ================= |akun, Favs, retweet per tweet|
    	 */
    	Configuration conf = new Configuration();
    	/**
    	 * Remove directory output if existing
    	 **/
    	FileSystem fs = FileSystem.get(conf);
    	if (fs.exists(new Path("data-out/adaptation_innovation"))) {
        	fs.delete(new Path("data-out/adaptation_innovation"), true);
		}
        Job job = Job.getInstance(conf, "Adaptation_innovation");
        job.setJarByClass(Adaptation_innovation_old.class);
        job.setMapperClass(TaskMapper.class);
        
        //MultipleInputs.addInputPath(job, IN_TWEET, TextInputFormat.class,TaskMapper.class);
        //MultipleInputs.addInputPath(job, OUT_RETWEET, TextInputFormat.class);
        
        /**
         * Combiner 
         **/
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TaskReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        
        
        FileInputFormat.addInputPath(job, new Path("data-in/adaptation_innovation"));
        FileOutputFormat.setOutputPath(job, new Path("data-out/adaptation_innovation"));
        /**
         * Runing Time hadoop JOB
         */
        job.waitForCompletion(true);     
        if (!job.isSuccessful()) {
          System.out.println("Job Finding adopt_inovation " + job.getJobID() + " failed!");
          System.exit(1);
        }
        final double duration = (System.currentTimeMillis() - startTime)/1000.0;
        System.out.println("Job Finished in " + duration + " seconds");
        System.exit(-1);
    }
}
