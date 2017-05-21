package edu.stanford.cs246.wordcount;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFIDF extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new TFIDF(), args);
      
      System.exit(res);
   }
   static List<Pair<String, Double>> words = new ArrayList<Pair<String, Double>>(); //list for ordering

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job1 = new Job(getConf(), "TFIDF");
      Job job2 = new Job(getConf(),"TFIDF");
      Job job3 = new Job(getConf(),"TFIDF");
      Job job4 = new Job(getConf(),"TFIDF");
      
	   
	   //job1 is a wordcount per doc
      job1.setJarByClass(TFIDF.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(IntWritable.class);
      job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      job1.setMapperClass(Map1.class);
      job1.setReducerClass(Reduce1.class);
      job1.setNumReduceTasks(10);
      job1.setInputFormatClass(TextInputFormat.class);
      job1.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path(args[1]));
      FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
      
	   //word in doc and wordcount
      if (job1.waitForCompletion(true)){
      job2.setJarByClass(TFIDF.class);
      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);
      job2.setMapperClass(Map2.class);
      job2.setReducerClass(Reduce2.class);
      job2.setNumReduceTasks(10);
      job2.setInputFormatClass(KeyValueTextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job2, new Path(args[1]));
      FileOutputFormat.setOutputPath(job2, new Path(args[2]));
      FileSystem fs1 = FileSystem.newInstance(getConf());
		if (fs1.exists(new Path(args[2]))) {
			fs1.delete(new Path(args[2]), true);
			}
      	}
      
	   //compute the TFIDF
      if (job2.waitForCompletion(true)){
          job3.setJarByClass(TFIDF.class);
          job3.setOutputKeyClass(Text.class);
          job3.setOutputValueClass(Text.class);
          job3.setMapperClass(Map3.class);
          job3.setReducerClass(Reduce3.class);
          job3.setNumReduceTasks(10);
          job3.setInputFormatClass(KeyValueTextInputFormat.class);
          job3.setOutputFormatClass(TextOutputFormat.class);
          FileInputFormat.addInputPath(job3, new Path(args[2]));
          FileOutputFormat.setOutputPath(job3, new Path(args[3]));
          FileSystem fs1 = FileSystem.newInstance(getConf());
    		if (fs1.exists(new Path(args[3]))) {
    			fs1.delete(new Path(args[3]), true);
    			}
          job3.waitForCompletion(true);
          	}
      
	   
	   //order the results
      if (job3.waitForCompletion(true)){
          job4.setJarByClass(TFIDF.class);
          job4.setOutputKeyClass(Text.class);
          job4.setOutputValueClass(Text.class);
          job4.setMapperClass(Map4.class);
          job4.setReducerClass(Reduce4.class);
          job4.setNumReduceTasks(10);
          job4.setInputFormatClass(KeyValueTextInputFormat.class);
          job4.setOutputFormatClass(TextOutputFormat.class);
          FileInputFormat.addInputPath(job4, new Path(args[3]));
          FileOutputFormat.setOutputPath(job4, new Path(args[4]));
          FileSystem fs1 = FileSystem.newInstance(getConf());
    		if (fs1.exists(new Path(args[4]))) {
    			fs1.delete(new Path(args[4]), true);
    			}
          job4.waitForCompletion(true);
          	}
      return 0;
   }
   
   public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
	  private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();
      private Text filename = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
    	  
    	  
    	  String filenameStr = ((FileSplit) context.getInputSplit())
					.getPath().getName();
			filename = new Text(filenameStr);
	
	      //put everything in lower case and separate words from punctuations
    	  StringTokenizer tokenizer = new StringTokenizer(value.toString(), " \t\n\r\f,.:;?![]'#--()_\"*/$%&<>+=@"); 
    	  while (tokenizer.hasMoreTokens()) { 
    		  String stri = tokenizer.nextToken().toLowerCase();
    			
    		  word.set(stri); 
    		  StringBuilder Stringbuilder = new StringBuilder();
    		  Stringbuilder.append(word +","+ filename); //write the pairs separated by a comma
				context.write(new Text(Stringbuilder.toString()), ONE); 
    			
         }
      }
   }

   public static class Reduce1 extends Reducer<Text, IntWritable, Text, Integer> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		
			 int sum = 0;
	         for (IntWritable val : values) {
			
			 //for each word, compute its number of occurences in the doc
	        	 sum += val.get();
	        	 }
			context.write(key, sum);

		}
	}



public static class Map2 extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
  	  
	    //split doc and word
    	String[] parts = key.toString().split(",");
    		String key1 = parts[0];
    		String key2 = parts[1];
		  
	    //output (doc,[word,countperdoc])
		  StringBuilder StringBuilder = new StringBuilder();
		  StringBuilder.append(key1+","+value);
		  context.write(new Text(key2),new Text(StringBuilder.toString()));
       }
    }
 
 public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
	 
	    @Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
	    	int sum = 0;
	    	List<Text> cache = new ArrayList<Text>();
		
		//sum the occurences of each words in the doc to have a Wordcount
	    	for (Text value : values) {
				String[] parts = value.toString().split(",");
	    		String key2 = parts[1];
	    		sum += Integer.parseInt(key2);
			
			//to iterate over it again
	    		cache.add(new Text(value));
	    	}
	    	
		//output ([word,doc],[countperdoc,wordcount])
	    	for (Text value1 : cache) {
				String[] parts2 = value1.toString().split(",");
				String key1 = parts2[0];
	    		String key2 = parts2[1];
	    		StringBuilder StringBuilder = new StringBuilder();
	    		StringBuilder.append(key1+","+key);
	    		StringBuilder StringBuilder2 = new StringBuilder();
	    		StringBuilder2.append(key2+","+sum);
	    		context.write(new Text(StringBuilder.toString()),new Text(StringBuilder2.toString()));
			}
    		
			
		}

 }
 public static class Map3 extends Mapper<Text, Text, Text, Text> {

	    @Override
	    public void map(Text key, Text value, Context context)
	            throws IOException, InterruptedException {
	  	  
	    	String[] parts = key.toString().split(",");
	    		String key1 = parts[0];
	    		String key2 = parts[1];
			  
			  StringBuilder StringBuilder = new StringBuilder();
			  StringBuilder.append(key2+","+value);
			  context.write(new Text(key1),new Text(StringBuilder.toString()));
	       }
	    }
	 
	 public static class Reduce3 extends Reducer<Text, Text, Text, Double> {
		 
		    @Override
			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
				
		    	int docperword = 0;
		    	List<Text> cache = new ArrayList<Text>();
		    	
		    	for (Text value : values) {
		    		docperword += 1;
		    		cache.add(new Text(value));
		    	}
		    		
		    	for (Text value1 : cache) {
					String[] parts2 = value1.toString().split(",");
					
					String docID = parts2[0];
					
		    		String WordCount = parts2[1];
		    		int WordCount1 = Integer.parseInt(WordCount);
		    		
		    		String WordsinDoc = parts2[2];
		    		int WordsinDoc1 = Integer.parseInt(WordsinDoc);
		    		
		    		StringBuilder StringBuilder = new StringBuilder();
		    		StringBuilder.append(key+","+docID);
		    		double tf = (double) WordCount1/WordsinDoc1;
		    		double idf =(double) Math.log(2/docperword);
		    		double tfidf = tf*idf;
		    		context.write(new Text(StringBuilder.toString()),tfidf);
				}
	    		
				
			}

	 }
	 public static class Map4 extends Mapper<Text, Text, Text, Text> {

		    @Override
		    public void map(Text key, Text values, Context context)
		            throws IOException, InterruptedException {
		  	  
		    		String parts = key.toString();
					words.add(new Pair<String, Double>(parts,Double.parseDouble(values.toString()))); 
					context.write(new Text("test"),new Text("test"));
				
				
		    	}
	 	}    
	 
	 public static class Reduce4 extends Reducer<Text, Text, Text, Text> {
		 
		    @Override
			public void reduce(Text key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {
		    	
		    	Collections.sort(words, new Comparator<Pair<String, Double>>() {
				    @Override
				    public int compare(final Pair<String, Double> o1, final Pair<String, Double> o2) {
						return -Double.compare(o1.getSecond(),o2.getSecond());
				    }
				    });
						
		    			System.out.println(words.size());
						for (int i = 0; i<words.size();i++) { //i<words.size() because we start from 0
							StringBuilder Stringbuilder = new StringBuilder();
							Stringbuilder.append((words.get(i)).getSecond()); 
							context.write(new Text(words.get(i).getFirst()),new Text (Stringbuilder.toString()));
						}
						

				    }
				
		    }
	 	
	}

