import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException; 	
import java.io.PrintStream;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

	public class KMeans {
		
		static ArrayList<ArrayList<Double>> clist;
		static File  file ;
		
		static ArrayList<Double> costList = new ArrayList<Double>();
		
		static double cost=0.0;
	   public static class kMap extends Mapper<LongWritable, Text, IntWritable, Text> {
		   
// 	     private final static IntWritable one = new IntWritable(1);
// 	     private Text word = new Text();
 	
 	     public void map(LongWritable key, Text value, Context context) throws IOException,FileNotFoundException,InterruptedException {
 	       
 	    	 
// 	    	 String[] line = value.toString().split(" ");
// 	    	 
// 	    	 System.out.println(line.length);
 	    	 
 	    	 
 	    	
 	    
 	    	
 	    	
 	    	ArrayList<Double> distances = new ArrayList<Double>();
 	    	ArrayList<Double> costdistances = new ArrayList<Double>();
 	    	String line = value.toString();
 	    	
 	    	
 	    	
 	    	String[] tempPoint = line.split(" ");
 	    	ArrayList<Double> point = new ArrayList<Double>();
 	    	
 	    	for (String x : tempPoint){
 	    		
 	    		point.add(Double.parseDouble(x));
 	    	}
 	    	
 	    	for(int i =0 ; i < clist.size() ; i++ ){
 	    		ArrayList<Double> centroid = clist.get(i);
 	    		Double tempDist = 0.0;
 	    		for(int j=0; j < point.size() ; j++){
 	    			tempDist += Math.pow(centroid.get(j) - point.get(j) , 2);
 	    		
 	    		}
 	    		
 	    		distances.add(Math.sqrt(tempDist));
 	    		costdistances.add(tempDist);
 	    	}
 	    	
 	    	int clusterIndex = distances.indexOf(Collections.min(distances));
 	    	cost = cost + Collections.min(costdistances);
 	    	
 	    	
 	    	
 	    	
 	    	context.write(new IntWritable(clusterIndex), value);

 	     }
 	   }
 	
 	   public static class kReduce extends Reducer<IntWritable,Text, Text, Text> {
 		   
 	     public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException {
 	       
 	    	 
 	    	
 	    	 
 	    	 ArrayList<ArrayList<Double>> cluster = new ArrayList<ArrayList<Double>>();
 	    	 
 	    	 for(Text t : values){
 	    		 ArrayList<Double> temp = new ArrayList<Double>();
 	    		 String line = t.toString();
 	    		 String[] point = line.split(" ");
 	    		 
 	    		 for (String x : point){
 	    			 temp.add(Double.parseDouble(x));
 	    		 }
 	    		 
 	    		 cluster.add(temp);
 	    	 }
 	    	 
 	    	 
 	    	 int length = cluster.size();
 	    	 
 	    	 double[] centroid = new double[58];
 	    	 
 	    	 
// 	    	 System.out.println(centroid[0]);
 	    	 
 	    	 for(int i =0 ; i < length ; i++){
 	    		 
 	    		 for (int j = 0 ; j < cluster.get(0).size(); j++){
 	    			 
// 	    			 System.out.println(i + " " + j);
 	    			 centroid[j] = centroid[j] +  cluster.get(i).get(j);
 	    		 }
 	    	 }
 	    	 
 	    	 for(int i = 0; i< centroid.length ; i++){
 	    		 centroid[i] = centroid[i]/length;
 	    	 }
 	    	 
 	    	 String newCent = "";
 	    	 
 	    	 for(double d : centroid){
 	    		 
 	    		 newCent = newCent + d + " ";
 	    	 }
 	    	 
 	    	 context.write(new Text(""), new Text(newCent));
 	    	 
 	    	 
 	     }
 	   }
 	
 	   public static void main(String[] args) throws Exception {
 		  
 		   
 		   for(int i =0 ; i <20; i++){
 			   cost = 0.0;
 			   if(i == 0 ){
 				  file  = new File("c2.txt");
 			   }
 			   else{
 				   
 				   file = new File("output/part-r-00000");
 			   }
 		   
 		   
	    	
 		   
 		   
 		   
 		   
 		   
 		   clist = new ArrayList<ArrayList<Double>>();
	    	
	    	
	    	BufferedReader reader = new BufferedReader(new FileReader(file));
	    	String text = null;
	    	
	    	while( (text = reader.readLine()) != null ){
	    		String[] temp = text.split(" ");
	    		
	    		ArrayList<Double> temp1 = new ArrayList<Double>();
	    		
	    		for (String x : temp){
	    			temp1.add(Double.parseDouble(x));
	    		}
	    		
	    		clist.add(temp1);
	    	}
	    	System.out.println(clist.get(0));
 		 
 		 Configuration conf = new Configuration();

 	     Job job = new Job(conf, "KMeans");
 	     job.setJarByClass(KMeans.class);
 	     
 	     job.setMapperClass(kMap.class);
// 	     job.setCombinerClass(Reduce.class);
 	     job.setReducerClass(kReduce.class);
 	     
 	     
 	     job.setMapOutputKeyClass(IntWritable.class);
 	     job.setMapOutputValueClass(Text.class);
 	     
 	     job.setOutputKeyClass(Text.class);
 	     job.setOutputValueClass(Text.class);
 	

 	
 	     job.setInputFormatClass(TextInputFormat.class);
 	     job.setOutputFormatClass(TextOutputFormat.class);
 	
 	     
 	     FileSystem outFs = new Path(args[1]).getFileSystem(conf);
 	     outFs.delete(new Path(args[1]), true);
        
 	     FileInputFormat.addInputPath(job, new Path(args[0]));
	     FileOutputFormat.setOutputPath(job, new Path(args[1]));

	     job.waitForCompletion(true);
	     
	     //append write cost to costfile.txt
	     
	     costList.add(cost);
	     
 		 }
 		   
 		 for (double c: costList){
 			 System.out.println(c);
 		 }
 	   }
 	}