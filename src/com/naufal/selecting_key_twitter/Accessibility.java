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

/**
 * 
 * @author ahmadluky
 * Chain Job Mapreduce
 */
public class Accessibility {
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
    //mention |mention|
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
    // quote |akun, quote|
 	public static class TaskMapper_quote extends Mapper<Object, Text, Text, IntWritable>{
     	private Text word = new Text();
     	private IntWritable one = new IntWritable(1);
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 			word.set(value);
 			context.write(word, one);
         }
     }
    public static class TaskReducer_quote extends Reducer<Text,IntWritable,Text,IntWritable> {
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
    // jumlsh kelas positif
    // jumlah kelas negatif
    // |mention, kelas|
  	public static class TaskMapper_sentimen extends Mapper<Object, Text, Text, IntWritable>{
      	private Text word = new Text();
     	private IntWritable one = new IntWritable(1);
		private String tagPos = "~POS";
		private String tagNeg = "~NEG";
  		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  			String[] sentimen = value.toString().split(";");
  			if (sentimen.length==2) {
				String tString = sentimen[0].toString().replace('"', ' ');
				String tString2 = tString.replaceAll("\\s+","");
	        	String[] split = tString2.split(",");
	        	if ( split.length>0 ) {
	        		for (int i = 0; i < split.length; i++) {
	        			if (!split[i].equalsIgnoreCase(" ")) {
			        		if (sentimen[1].equals("positif")){
		        				word.set(split[i]+tagPos);
				    			context.write(word, one);
			        		}
		        			if (sentimen[1].equals("negatif")){
		        				word.set(split[i]+tagNeg);
				    			context.write(word, one);
		        			}
						}
					}
				}
			}
         }
     }
    public static class TaskReducer_sentimen extends Reducer<Text,IntWritable,Text,IntWritable> {
          private IntWritable result = new IntWritable();
          public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          	  int sum = 0;
          	  String[] sentimen_key = key.toString().split("~");
          	  if (sentimen_key[1].equals("POS")) {
                  for (IntWritable val : values) {
                	  sum += val.get();
                  }
                  result.set(sum);
                  context.write(key, result);
          	  }
          	  if (sentimen_key[1].equals("NEG")) {
                  for (IntWritable val : values) {
                	  sum += val.get();
                  }
                  result.set(sum);
                  context.write(key, result);
        	  }
          }
    }
    // accessibility
	// sentimen accessibility
    public static class TaskMapper_accessibilitySentiment extends Mapper<Object, Text, Text, Text>{
     	private Text word = new Text();
 		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         	String[] split = value.toString().split("\t");
         	String[] senti = split[0].split("~");
         	if (senti[1].equals("POS")) {
     			word.set(senti[0]);
     			context.write(word, new Text(split[1]+"~POS"));
			}
         	if (senti[1].equals("NEG")) {
     			word.set(senti[0]);
     			context.write(word, new Text(split[1]+"~NEG"));
			}
         }
    }
	// quote accessibility 
    public static class TaskMapper_accessibilityQuote extends Mapper<Object, Text, Text, Text>{
     	private Text word = new Text();
		private String fileTag = "~QUOTE";
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
        	Double pos = 0.0;
        	Double neg = 0.0;
        	Double quote = 0.0;
        	Double mention = 0.0;
        	Double reply = 0.0;
        	Double following = 0.0;
            for (Text val : values) {
	           	String[] split = val.toString().split("~");
	           	if (split[1].equals("QUOTE")) {
	           		quote = Double.parseDouble(split[0]);
	           	}
	           	if (split[1].equals("MENTION")) {
	           		mention = Double.parseDouble(split[0]);
	           	}
	           	if (split[1].equals("REPLY")) {
	           		reply = Double.parseDouble(split[0]);
	           	}
	           	if (split[1].equals("FOLLOWING")) {
	           		following = Double.parseDouble(split[0]);
	           	}
	           	if (split[1].equals("POS")) {
	           		pos = Double.parseDouble(split[0]);
	           	}
	           	if (split[1].equals("NEG")) {
					neg = Double.parseDouble(split[0]);
	            }
	           	if (neg==0.0) {
						neg=1.0;
	           	}
            }
           	rst = following + reply * ( (quote + mention) * (pos / neg));
            result.set(rst);
            context.write(key, result);
        }
    }
    // nilai_aksesibilitas = folowing(i)+jml_reply(i)+[(quote(i)+mention(i)) * (jumlah akun pos/jumlah akun neg)]
    // 1. get jml_reply
    // 2. get quote
    // 3. get mention
    // 4. get jumlah akun pos -
    // 5. get jumlah akun neg -
    private static void usage() {
    	// data-in/accessibility/sentimen data-out/accessibility/sentimen data-in/accessibility/quote data-out/accessibility/quote data-in/accessibility/mention
    	// data-out/accessibility/mention data-in/accessibility/reply data-out/accessibility/reply data-in/accessibility/following data-out/accessibility/out
        System.err.println("usage: hadoop -jar <...> Accessibility <input sentimen> <output sentiment> "
        					+ "<input quote> <output quote> <input mention> <output mention>"
        					+ "<input reply> <output reply> <input following> <output result>");
        System.exit(-1);
    }
    public static void main(String[] args) throws Exception {
		if (args.length != 10)
	        usage();

        System.out.println("Starting Job");
        long startTime = new Date().getTime();
        
    	/**
    	 * get sentiment akun
    	 */
    	Configuration conf_sentiment = new Configuration();
		String[] dir=new GenericOptionsParser(conf_sentiment,args).getRemainingArgs();
		
    	String sentiment_in = dir[0];
    	String sentiment_out = dir[1];
    	
    	FileSystem fs_sentiment = FileSystem.get(conf_sentiment);
    	if (fs_sentiment.exists(new Path(sentiment_out))) {
        	fs_sentiment.delete(new Path(sentiment_out), true);
		}
        Job job_sentiment = Job.getInstance(conf_sentiment, "sentimen_count");
        job_sentiment.setJarByClass(Accessibility.class);
        job_sentiment.setMapperClass(TaskMapper_sentimen.class);
        job_sentiment.setCombinerClass(IntSumReducer.class);
        job_sentiment.setReducerClass(TaskReducer_sentimen.class);
        job_sentiment.setOutputKeyClass(Text.class);
        job_sentiment.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_sentiment, new Path(sentiment_in));
        FileOutputFormat.setOutputPath(job_sentiment, new Path(sentiment_out));
        job_sentiment.waitForCompletion(true);     
        if (!job_sentiment.isSuccessful()) {
        	System.out.println("Job Finding Local " + job_sentiment.getJobID() + " failed!");
        	System.exit(1);
        }
    	/**
    	 * get quote
    	 */
    	String quote_in = dir[2];
    	String quote_out = dir[3];
    	Configuration conf_quote = new Configuration();
    	FileSystem fs_quote = FileSystem.get(conf_quote);
    	if (fs_quote.exists(new Path(quote_in))) {
        	fs_quote.delete(new Path(quote_out), true);
		}
        Job job_quote = Job.getInstance(conf_quote, "quote_count");
        job_quote.setJarByClass(Accessibility.class);
        job_quote.setMapperClass(TaskMapper_quote.class);
        job_quote.setCombinerClass(IntSumReducer.class);
        job_quote.setReducerClass(TaskReducer_quote.class);
        job_quote.setOutputKeyClass(Text.class);
        job_quote.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_quote, new Path(quote_in));
        FileOutputFormat.setOutputPath(job_quote, new Path(quote_out));
        job_quote.waitForCompletion(true);     
        if (!job_quote.isSuccessful()) {
        	System.out.println("Job Finding Local " + job_quote.getJobID() + " failed!");
        	System.exit(1);
        }
    	/**
    	 * get mention
    	 */
        String mention_in = dir[4];
    	String mention_out = dir[5];
    	Configuration conf_mention = new Configuration();
    	FileSystem fs_mention = FileSystem.get(conf_mention);
    	if (fs_mention.exists(new Path(mention_in))) {
        	fs_mention.delete(new Path(mention_out), true);
		}
        Job job_mention = Job.getInstance(conf_mention, "Reply_count");
        job_mention.setJarByClass(Accessibility.class);
        job_mention.setMapperClass(TaskMapper_mention.class);
        job_mention.setCombinerClass(IntSumReducer.class);
        job_mention.setReducerClass(TaskReducer_mention.class);
        job_mention.setOutputKeyClass(Text.class);
        job_mention.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_mention, new Path(mention_in));
        FileOutputFormat.setOutputPath(job_mention, new Path(mention_out));
        job_mention.waitForCompletion(true);     
        if (!job_mention.isSuccessful()) {
        	System.out.println("Job Finding Local " + job_mention.getJobID() + " failed!");
        	System.exit(1);
        }
    	/**
    	 * get jumlah_reply
    	 */
        String jumlah_reply_in = dir[6];
    	String jumlah_reply_out = dir[7];
    	Configuration conf_reply = new Configuration();
    	FileSystem fs_reply = FileSystem.get(conf_reply);
    	if (fs_reply.exists(new Path(jumlah_reply_in))) {
        	fs_reply.delete(new Path(jumlah_reply_out), true);
		}
        Job job_reply = Job.getInstance(conf_reply, "Reply_count");
        job_reply.setJarByClass(Accessibility.class);
        job_reply.setMapperClass(TaskMapper_reply.class);
        job_reply.setCombinerClass(IntSumReducer.class);
        job_reply.setReducerClass(TaskReducer_reply.class);
        job_reply.setOutputKeyClass(Text.class);
        job_reply.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job_reply, new Path(jumlah_reply_in));
        FileOutputFormat.setOutputPath(job_reply, new Path(jumlah_reply_out));
        job_reply.waitForCompletion(true);     
        if (!job_reply.isSuccessful()) {
          System.out.println("Job Finding Local " + job_reply.getJobID() + " failed!");
          System.exit(1);
        }
    	/**
    	 * accessibility
    	 */
        String FOLLOWING = dir[8];
    	String OUT = dir[9];
    	Configuration conf = new Configuration();
    	FileSystem fs = FileSystem.get(conf);
    	if (fs.exists(new Path(OUT))) {
        	fs.delete(new Path(OUT), true);
		}
		Path p1=new Path(sentiment_out); 
		Path p2=new Path(quote_out);
		Path p3=new Path(jumlah_reply_out);
		Path p4=new Path(mention_out);
		Path p5=new Path(FOLLOWING);
		Path p6=new Path(OUT);
        Job job = Job.getInstance(conf, "Accessibility");
        job.setJarByClass(Accessibility.class);
        job.setMapperClass(TaskMapper_accessibilitySentiment.class);
        job.setMapperClass(TaskMapper_accessibilityQuote.class);
        job.setMapperClass(TaskMapper_accessibilityReply.class);
        job.setMapperClass(TaskMapper_accessibilityMention.class);
        job.setMapperClass(TaskMapper_accessibilityFollowing.class);
        job.setReducerClass(TaskReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, TaskMapper_accessibilitySentiment.class);
		MultipleInputs.addInputPath(job, p2, TextInputFormat.class, TaskMapper_accessibilityQuote.class);
		MultipleInputs.addInputPath(job, p3, TextInputFormat.class, TaskMapper_accessibilityReply.class);
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
