package edu.stanford.cs246.wordcount;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Pagerank extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Pagerank(), args);
      System.exit(res);
   }
   
   static int iterations = 0;
   static int total = 0;
   static HashSet<String> hola = new HashSet<String>();
   static List<Pair<String, Double>> words = new ArrayList<Pair<String, Double>>();
   
   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job1 = new Job(getConf(), "Pagerank");
      job1.setJarByClass(Pagerank.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);
      job1.setMapperClass(Map1.class);
      job1.setReducerClass(Reduce1.class);
      job1.setNumReduceTasks(10);
      job1.setInputFormatClass(KeyValueTextInputFormat.class);
      job1.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path(args[1]));
      FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
	
		  if (job1.waitForCompletion(true)){
			  while (total != hola.size()){
				  total=0;
			  Job job2 = new Job(getConf(),"Pagerank");
			  job2.setJarByClass(Pagerank.class);
			  job2.setOutputKeyClass(Text.class);
			  job2.setOutputValueClass(Text.class);
			  job2.setMapperClass(Map2.class);
			  job2.setReducerClass(Reduce2.class);
			  job2.setNumReduceTasks(10);
			  job2.setInputFormatClass(KeyValueTextInputFormat.class);
			  job2.setOutputFormatClass(TextOutputFormat.class);
			  	if (iterations == 0)
			  FileInputFormat.addInputPath(job2, new Path(args[1]));
			  else if (iterations % 2 == 1)
				  FileInputFormat.addInputPath(job2, new Path(args[2]));
			  else if (iterations % 2 == 0)
				  FileInputFormat.addInputPath(job2, new Path(args[3]));
			  FileSystem fs1 = FileSystem.newInstance(getConf());
			  FileSystem fs2 = FileSystem.newInstance(getConf());
			  
			  	//initially no output folder so create one
			  	if (iterations % 2 == 0){
			  		if (fs1.exists(new Path(args[2]))) {
						fs1.delete(new Path(args[2]), true);
					}
			  		 FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			  	}
			  	
			  	// after first output folder create another one for second iteration
			  	else if (iterations % 2 == 1){
			  		if (fs2.exists(new Path(args[3]))) {
						fs2.delete(new Path(args[3]), true);
					}
			  		 FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			  	}
			  
			  	if (job2.waitForCompletion(true)){
			  iterations += 1;
			  System.out.println(iterations);}
		  	
			  			
						  if (iterations > 1){
						  Job job4 = new Job(getConf(),"Pagerank");
				          job4.setJarByClass(Pagerank.class);
				          job4.setOutputKeyClass(Text.class);
				          job4.setOutputValueClass(Text.class);
				          job4.setReducerClass(Reduce4.class);
				          job4.setNumReduceTasks(10);
				          job4.setInputFormatClass(KeyValueTextInputFormat.class);
				          job4.setOutputFormatClass(TextOutputFormat.class);
				          MultipleInputs.addInputPath(job4, new Path(args[2]),KeyValueTextInputFormat.class,Map4.class);
				          MultipleInputs.addInputPath(job4, new Path(args[3]),KeyValueTextInputFormat.class,Map41.class);
				          FileOutputFormat.setOutputPath(job4, new Path(args[4]));
				          FileSystem fs3 = FileSystem.newInstance(getConf());
				    		if (fs3.exists(new Path(args[4]))) {
				    			fs3.delete(new Path(args[4]), true);
				    			}
				         
				          job4.waitForCompletion(true);
						  }
			  
			  if (total == hola.size()){
			   
			  Job job5 = new Job(getConf(),"Pagerank");
	          job5.setJarByClass(Pagerank.class);
	          job5.setOutputKeyClass(Text.class);
	          job5.setOutputValueClass(Text.class);
	          job5.setMapperClass(Map5.class);
	          job5.setReducerClass(Reduce5.class);
	          job5.setNumReduceTasks(10);
	          job5.setInputFormatClass(KeyValueTextInputFormat.class);
	          job5.setOutputFormatClass(TextOutputFormat.class);
	          	if (iterations % 2 == 0){
	          FileInputFormat.addInputPath(job5, new Path(args[2]));}
	          	else FileInputFormat.addInputPath(job5, new Path(args[3]));
	          FileOutputFormat.setOutputPath(job5, new Path(args[5]));
	          FileSystem fs4 = FileSystem.newInstance(getConf());
	    		if (fs4.exists(new Path(args[5]))) {
	    			fs4.delete(new Path(args[5]), true);
	    			}
	         
	          job5.waitForCompletion(true);
			  }
		  }
		  }
      return 0;
   }
   
   public static class Map1 extends Mapper<Text, Text, Text, Text> {

      @Override
      public void map(Text key, Text value, Context context)
              throws IOException, InterruptedException {
    	String val = value.toString();
    	context.write(key,new Text(val));
    	hola.add(val);
    	hola.add(key.toString());
      }
   }

   public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashSet<String> output = new HashSet<String>();
			
			double infl =(double) 1/hola.size();
			for (Text value : values) {
				output.add(value.toString());
				
			}
			
			StringBuilder Stringbuilder = new StringBuilder();
			boolean isfirst = true;
			for (String value : output) {
				if (!isfirst){
				Stringbuilder.append("/");
				}
				isfirst = false;
				Stringbuilder.append(value);
			}
			
			StringBuilder Stringbuilder2 = new StringBuilder();
			Stringbuilder2.append(key+","+infl);
			
			
			context.write(new Text(Stringbuilder2.toString()), new Text(Stringbuilder.toString()));
		}
	}



public static class Map2 extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text values, Context context)
            throws IOException, InterruptedException {
  	  
    		String[] parts = key.toString().split(",");
    		String key1 = parts[0];
    		String key2 = parts[1];
    		
    		
			String[] parts2 = values.toString().split("/");
			for (String value : parts2) {
				if (values.getLength()==0){
					
				}
				else{
	    		double influence = (double)Double.parseDouble(key2)/parts2.length;
	    		StringBuilder bla1 = new StringBuilder();
	    		bla1.append(influence);
	    		context.write(new Text(value),new Text(bla1.toString()));
				}
			}
			context.write(new Text(key1),values);
		
       }
    }
 
 public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
	 
	    @Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
	    	
	    	double pagerank = 0;
	    	double dumping = 0.85;
	    	int N = hola.size();
	    	double influence = 0;
	    	String linkout = new String();
	    	
	    	
	    	for (Text value : values) {
				if (value.toString().contains("/") ){
					linkout=value.toString();	
					
				}
				else if (value.toString().isEmpty()) {
					
				}
				else if (Double.parseDouble(value.toString())>1){
					linkout=value.toString();	
					
				}
	    		else influence += Double.parseDouble(value.toString());
				
	    	}
	    	
	    	pagerank=((1-dumping)/N)+(dumping*influence);
	    	StringBuilder key1 = new StringBuilder();
	    	key1.append(key+","+pagerank);
	    	context.write(new Text(key1.toString()),new Text(linkout));
	    	
	    }
 }
 
 public static class Map4 extends Mapper<Text, Text, Text, Text> {

		    @Override
		    public void map(Text key, Text values, Context context)
		            throws IOException, InterruptedException {
		  	  
		    		String[] parts = key.toString().split(",");
		    		String key1 = parts[0];
		    		String key2 = parts[1];;
			    	context.write(new Text(key1),new Text(key2));
			}
				
				
 }
 public static class Map41 extends Mapper<Text, Text, Text, Text> {

	    @Override
	    public void map(Text key, Text values, Context context)
	            throws IOException, InterruptedException {
	  	  
	    		String[] parts = key.toString().split(",");
	    		String key1 = parts[0];
	    		String key2 = parts[1];;
		    	context.write(new Text(key1),new Text(key2));
		}
			
			
}
		    
 public static class Reduce4 extends Reducer<Text, Text, Text, Text> {
			 
			    @Override
				public void reduce(Text key, Iterable<Text> values, Context context)
						throws IOException, InterruptedException {
			    	
			    	List<Double> ranks = new ArrayList<Double>();
			    	for (Text value : values) {
			    		double pagerank = Double.parseDouble(value.toString());
			    		ranks.add(pagerank);
			    	}
			    	double diff = Math.abs(ranks.get(0)-ranks.get(1));
			    	if (diff<0.0001) total += 1;
			    	
			    }
		 }
 public static class Map5 extends Mapper<Text, Text, Text, Text> {

	    @Override
	    public void map(Text key, Text values, Context context)
	            throws IOException, InterruptedException {
	  	  
	    		String[] parts = key.toString().split(",");
	    		String page = parts[0];
	    		String rank = parts[1];
				words.add(new Pair<String, Double>(page,Double.parseDouble(rank))); 
				context.write(new Text("test"),new Text("test"));
			
			
	    	}
 	}    
 
 public static class Reduce5 extends Reducer<Text, Text, Text, Text> {
	 
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
