package com.naufal.selecting_key_facebook;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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

public class Group_weight {
	
	public static class Weight {
			public String FileName;
			public String delimiter="\\t";
			private BufferedReader br;
			public Weight(){
				this.FileName = "./data-out/GrpW/part-r-00000";
			}
			public Map<String,Double> read() throws IOException{
				br = new BufferedReader(new FileReader(this.FileName));
				String sCurrentLine;
				Map<String,Double> map = new HashMap<String,Double>();
				while ((sCurrentLine = br.readLine()) != null) {
					String[] value = sCurrentLine.split(delimiter);
					map.put(value[0], Double.parseDouble(value[1]));
				}
				return map;
			}
			@SuppressWarnings("rawtypes")
			public void Calculation(Map<String,Double> value){
		        DecimalFormat df = new DecimalFormat("#.##");
				Double total = value.get("Total");
			    Set set = value.entrySet();
			    Iterator iterator = set.iterator();
			    while(iterator.hasNext()) {
			        Map.Entry valuetry = (Map.Entry)iterator.next();
			        if (!valuetry.getKey().equals("Total")) {
				        System.out.println( valuetry.getKey()+":"+ df.format((Double)valuetry.getValue()/total));
					}
			    }
			}
	}
	public static class TaskMapper extends Mapper<Object, Text, Text, IntWritable>{
	        private final static IntWritable one = new IntWritable(1);
	        private Text rows = new Text();
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        	StringTokenizer itr = new StringTokenizer(value.toString());
	            while (itr.hasMoreTokens()) {
	            	rows.set(itr.nextToken());
	            	String row = rows.toString();
	            	String[] G = row.split(";");
	        		Text GID = new Text(G[1]);
	        		context.write(GID, one);
	        		
	        		Text Total = new Text("Total");
	        		context.write(Total, one);
	            }
	        }
	}
	public static class TaskReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		    private IntWritable result = new IntWritable();
		    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		        int sum=0;
		        for (IntWritable val : values) {
		        	sum +=val.get();
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
			if (fs.exists(new Path("data-out/GrpW"))) {
		    	fs.delete(new Path("data-out/GrpW"), true);
			}
			
			conf.set("mapred.textoutputformat.separatorText", ";");
			
		    Job job = Job.getInstance(conf, "Group_weight");
		    job.setJarByClass(Friend_pow.class);
		    job.setMapperClass(TaskMapper.class);
		    /**
		     * Combiner 
		     **/
		    job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(TaskReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path("data-in/GrpW"));
		    FileOutputFormat.setOutputPath(job, new Path("data-out/GrpW"));
		    /**
		     * Runing Time hadoop JOB
		     */
		    long start = new Date().getTime();
		    boolean status = job.waitForCompletion(true);
		    if (status) {
		    	Weight w = new Weight();
		    	Map<String,Double> rst = w.read();
		    	w.Calculation(rst);
			}
		    long end = new Date().getTime();
		    System.out.println("Job took "+(end-start) + " milliseconds");
		    System.exit(status ? 0 : 1);
		    
	}
}
