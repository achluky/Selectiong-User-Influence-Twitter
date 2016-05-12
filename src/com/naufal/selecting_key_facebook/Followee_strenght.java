package com.naufal.selecting_key_facebook;
import java.io.IOException;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class Followee_strenght {
	 public static class TaskMapper extends Mapper<Object, Text, Text, IntWritable>{
	        private final static IntWritable one = new IntWritable(1);
	        private Text rows = new Text();
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        	StringTokenizer itr = new StringTokenizer(value.toString());
	            while (itr.hasMoreTokens()) {
	            	rows.set(itr.nextToken());
	            		String row = rows.toString();
	            		String[] uid = row.split(";");
	        		Text a_uidText = new Text(uid[0]);
	        		context.write(a_uidText, one);
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
		Configuration conf = new Configuration();
		/**
		 * Remove directory output if existing *
		 **/
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("data-out/follPw"))) {
	    	fs.delete(new Path("data-out/follPw"), true);
		}
	    Job job = Job.getInstance(conf, "Friend_pow");
	    job.setJarByClass(Friend_pow.class);
	    job.setMapperClass(TaskMapper.class);
	    /**
	     * Combiner 
	     **/
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(TaskReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path("data-in/follPw"));
	    FileOutputFormat.setOutputPath(job, new Path("data-out/follPw"));
	    /**
	     * Runing Time hadoop JOB
	     */
	    long start = new Date().getTime();
	    boolean status = job.waitForCompletion(true);            
	    long end = new Date().getTime();
	    System.out.println("Job took "+(end-start) + " milliseconds");
	    System.exit(status ? 0 : 1);
	}
}
