package com.naufal.selecting_key_twitter;

import java.io.IOException;
import java.util.Date;

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
/**
 * Code menentukan jumlah sentimen positif dan negatif dari tweet replay dari tweet suatu 
 * akun yang akan digunakan pada penentuan nilai adaptasi inovasi.
 * @author ahmadluky
 *
 */
public class LabelSentiment_Count {
	public static class TaskMapper extends Mapper<Object, Text, Text, IntWritable>{
    	private Text word = new Text();
    	private IntWritable one = new IntWritable(1);
    	private IntWritable zero = new IntWritable(0);
    	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] split = value.toString().split(";");
        	word.set(split[0]);
        	if (split[1].endsWith("-")) {
            	context.write(word, zero);
			} else {
				context.write(word, one);
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
    	 * Remove directory output if existing
    	 **/
    	FileSystem fs = FileSystem.get(conf);
    	if (fs.exists(new Path("data-out/LabelSentiment_Count"))) {
        	fs.delete(new Path("data-out/LabelSentiment_Count"), true);
		}
        Job job = Job.getInstance(conf, "LabelSentiment_Count");
        job.setJarByClass(Accessibility.class);
        job.setMapperClass(TaskMapper.class);
        /**
         * Combiner 
         **/
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TaskReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        /**
         * Example data is positif. Input Path and Output Path
         */
        FileInputFormat.addInputPath(job, new Path("data-in/LabelSentiment_Count"));
        FileOutputFormat.setOutputPath(job, new Path("data-out/LabelSentiment_Count"));
        /**
         * Runing Time hadoop JOB
         */

        System.out.println("Starting Job Local");
        long startTime = new Date().getTime();
        job.waitForCompletion(true);     
        if (!job.isSuccessful()) {
          System.out.println("Job Finding Local " + job.getJobID() + " failed!");
          System.exit(1);
        }
        final double duration = (System.currentTimeMillis() - startTime)/1000.0;
        System.out.println("Job Finished Local in " + duration + " seconds");
    }
    
}
