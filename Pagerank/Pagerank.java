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
   
   static int iterations = 0; // count the number of iterations
   static int total = 0; // used to check results for all links
   static HashSet<String> tot = new HashSet<String>(); //total number of pages (hashset to have them only once)
   static List<Pair<String, Double>> words = new ArrayList<Pair<String, Double>>(); //list for comparing results between iterations
   
   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
	   
	   // job1 is to reformat the data into the form ([Page,pagerank], outgoing links)
      Job job1 = new Job(getConf(), "Pagerank");
      job1.setJarByClass(Pagerank.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);
      job1.setMapperClass(Map1.class);
      job1.setReducerClass(Reduce1.class);
      job1.setInputFormatClass(KeyValueTextInputFormat.class);
      job1.setOutputFormatClass(TextOutputFormat.class);
      FileInputFormat.addInputPath(job1, new Path(args[0]));
      FileOutputFormat.setOutputPath(job1, new Path(args[1]));
      FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}
		
		
		  if (job1.waitForCompletion(true)){
			  //total number of good enough results is not equal to total number of pages
			  while (total != tot.size()){
				  //reinatialise total
				  total=0;
				  
			  /* job 2 is the main job, the mapper outputs (page(for each outgoing links of the key, influence of key) or (page,outgoing links)
			  the reducer outputs the update ([Page,pagerank], outgoing links) */  
			  Job job2 = new Job(getConf(),"Pagerank");
			  job2.setJarByClass(Pagerank.class);
			  job2.setOutputKeyClass(Text.class);
			  job2.setOutputValueClass(Text.class);
			  job2.setMapperClass(Map2.class);
			  job2.setReducerClass(Reduce2.class);
			  job2.setInputFormatClass(KeyValueTextInputFormat.class);
			  job2.setOutputFormatClass(TextOutputFormat.class);
				  // first iteration use the output from job1
			  	if (iterations == 0)
			  FileInputFormat.addInputPath(job2, new Path(args[1]));
				  //for all impair iterations use the output of pair iterations (i.e the previous output)
			  else if (iterations % 2 == 1)
				  FileInputFormat.addInputPath(job2, new Path(args[2]));
				  //for all pair iterations use the output of impair iterations
			  else if (iterations % 2 == 0)
				  FileInputFormat.addInputPath(job2, new Path(args[3]));
			  FileSystem fs1 = FileSystem.newInstance(getConf());
			  FileSystem fs2 = FileSystem.newInstance(getConf());
			  
			  	//for all pair iterations, write the output in args[2]
			  	if (iterations % 2 == 0){
			  		if (fs1.exists(new Path(args[2]))) {
						fs1.delete(new Path(args[2]), true);
					}
			  		 FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			  	}
			  	
			  	//for all pair iterations, write the output in args[3] to alternate output files
			  	else if (iterations % 2 == 1){
			  		if (fs2.exists(new Path(args[3]))) {
						fs2.delete(new Path(args[3]), true);
					}
			  		 FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			  	}
			  	
				  // increment the iteration
			  	if (job2.waitForCompletion(true)){
			  iterations += 1;
			  System.out.println(iterations);} // to see at what point we are
		  	
			  			// job4 is to compare outputs and stop the process if they converge enough
						  if (iterations > 1){ //once we have at least two outputs to compare
						  Job job4 = new Job(getConf(),"Pagerank");
				          job4.setJarByClass(Pagerank.class);
				          job4.setOutputKeyClass(Text.class);
				          job4.setOutputValueClass(Text.class);
				          job4.setReducerClass(Reduce4.class);
				          job4.setInputFormatClass(KeyValueTextInputFormat.class);
				          job4.setOutputFormatClass(TextOutputFormat.class);
						 	//two input paths
				          MultipleInputs.addInputPath(job4, new Path(args[2]),KeyValueTextInputFormat.class,Map4.class);
				          MultipleInputs.addInputPath(job4, new Path(args[3]),KeyValueTextInputFormat.class,Map41.class);
				          FileOutputFormat.setOutputPath(job4, new Path(args[4]));
				          FileSystem fs3 = FileSystem.newInstance(getConf());
				    		if (fs3.exists(new Path(args[4]))) {
				    			fs3.delete(new Path(args[4]), true);
				    			}
				         
				          job4.waitForCompletion(true);
						  }
				  
			  //once we finished and have a good enough result, job5 orders it
			  if (total == tot.size()){
			   
		  Job job5 = new Job(getConf(),"Pagerank");
	          job5.setJarByClass(Pagerank.class);
	          job5.setOutputKeyClass(Text.class);
	          job5.setOutputValueClass(Text.class);
	          job5.setMapperClass(Map5.class);
	          job5.setReducerClass(Reduce5.class);
	          job5.setInputFormatClass(KeyValueTextInputFormat.class);
	          job5.setOutputFormatClass(TextOutputFormat.class);
				  
		 //check which output was the final one
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
	      
	      //put page as key and value as val
    	String val = value.toString();
    	context.write(key,new Text(val));
	      
	 // add all the different pages to our HashSet
    	tot.add(val);
    	tot.add(key.toString());
      }
   }

   public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashSet<String> output = new HashSet<String>();
			
			// pagerank, initially 1/N
			double pg =(double) 1/tot.size();
			for (Text value : values) {
				output.add(value.toString());
				
			}
			
			// add each outgoing links separated by a "/"
			StringBuilder Stringbuilder = new StringBuilder();
			boolean isfirst = true;
			for (String value : output) {
				if (!isfirst){
				Stringbuilder.append("/");
				}
				isfirst = false;
				Stringbuilder.append(value);
			}
			
			// new key is [page,pagerank]
			StringBuilder Stringbuilder2 = new StringBuilder();
			Stringbuilder2.append(key+","+pg);
			
			// ([page,pagerank],outgoing links)
			context.write(new Text(Stringbuilder2.toString()), new Text(Stringbuilder.toString()));
		}
	}



public static class Map2 extends Mapper<Text, Text, Text, Text> {

    @Override
    public void map(Text key, Text values, Context context)
            throws IOException, InterruptedException {
	    
  	  	// split page and pagerank
    		String[] parts = key.toString().split(",");
    		String key1 = parts[0];
    		String key2 = parts[1];
    		
    			
			String[] parts2 = values.toString().split("/");
			for (String value : parts2) {
				
				//if no outgoing links do nothing
				if (values.getLength()==0){	
				}
				
				//else compute influence(PR/NBoutlinks) of current key(page)
				else{
	    		double influence = (double)Double.parseDouble(key2)/parts2.length;
	    		StringBuilder bla1 = new StringBuilder();
	    		bla1.append(influence);
					
			// for each outgoing links of current key, output the link and influence of current key
	    		context.write(new Text(value),new Text(bla1.toString()));
				}
			}
	    
	    		//output the current key and its outgoing links
			context.write(new Text(key1),values);
		
       }
    }
 
 public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
	 
	    @Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
	    	
	    	double pagerank = 0;
	    	double dumping = 0.85;
	    	int N = tot.size();
	    	double influence = 0;
	    	String linkout = new String();
	    	
	    	// for each value check if it's influence or the outgoing list
	    	for (Text value : values) {
				
				//if it is the list 
				if (value.toString().contains("/") ){
					linkout=value.toString();	
				}
			
				// if the current value is empty do nothing (no outgoing list)
				else if (value.toString().isEmpty()) {
					
				}
				
				// if it is the list with only one element
				else if (Double.parseDouble(value.toString())>1){
					linkout=value.toString();	
					
				}
			
			//else sum the influences
	    		else influence += Double.parseDouble(value.toString());
				
	    	}
	    	
		//compute pagerank
	    	pagerank=((1-dumping)/N)+(dumping*influence);
	    	StringBuilder key1 = new StringBuilder();
	    	key1.append(key+","+pagerank);
		
		//output in same format as input to iterate over it
	    	context.write(new Text(key1.toString()),new Text(linkout));
	    	
	    }
 }
 
 public static class Map4 extends Mapper<Text, Text, Text, Text> {

		    @Override
		    public void map(Text key, Text values, Context context)
		            throws IOException, InterruptedException {
		  	  	
			    	// find for each page its pagerank in one of the doc
		    		String[] parts = key.toString().split(",");
		    		String key1 = parts[0];
		    		String key2 = parts[1];
			    	context.write(new Text(key1),new Text(key2));
			}
				
				
 }
	
	//could have done it without this mapper but it is clearer in case we need to change something in one of the inputs
 public static class Map41 extends Mapper<Text, Text, Text, Text> {

	    @Override
	    public void map(Text key, Text values, Context context)
	            throws IOException, InterruptedException {
	  	  	
		    // find for each page its pagerank in the other doc
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
			    	
					//compute the difference between the pagerank of two iterations
			    	List<Double> ranks = new ArrayList<Double>();
			    	for (Text value : values) {
			    		double pagerank = Double.parseDouble(value.toString());
			    		ranks.add(pagerank);
			    	}
				//if the difference is low enough(arbitrarily set here) add 1 to total and if total = number of pages then we can stop iterating
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
		    		
		    		// add the page/pagerank in a list of pairs
				words.add(new Pair<String, Double>(page,Double.parseDouble(rank))); 
				context.write(new Text("test"),new Text("test"));
			
			
	    	}
 	}    
 
 public static class Reduce5 extends Reducer<Text, Text, Text, Text> {
	 
	    @Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
	    	
		// Compare the pagerank of our results and order them in descending order
	    	Collections.sort(words, new Comparator<Pair<String, Double>>() {
			    @Override
			    public int compare(final Pair<String, Double> o1, final Pair<String, Double> o2) {
					return -Double.compare(o1.getSecond(),o2.getSecond());
			    }
			    });
					
	    				// output the ordered list
					for (int i = 0; i<words.size();i++) { //i<words.size() because we start from 0
						StringBuilder Stringbuilder = new StringBuilder();
						Stringbuilder.append((words.get(i)).getSecond()); 
						context.write(new Text(words.get(i).getFirst()),new Text (Stringbuilder.toString()));
					}
					

			    }
			
	    }
 	
}
