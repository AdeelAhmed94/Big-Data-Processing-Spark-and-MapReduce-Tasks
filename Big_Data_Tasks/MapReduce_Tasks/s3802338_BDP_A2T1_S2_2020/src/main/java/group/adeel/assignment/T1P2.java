package group.adeel.assignment;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;


public class T1P2 {
	//Extending Mapper class	
	public static class MapperClass extends Mapper<Object, Text, Text,MapWritable> {
		private Text word = new Text();  
		private MapWritable nbMap = new MapWritable(); 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {		
			String cleanText = value.toString().replaceAll("\\s+", " ").trim();
			String[] words = cleanText.split("\\s+");  
			int wdCount = words.length;
			
			if(wdCount > 1) {
				for(int i = 0; i < wdCount; i++){
					word.set(words[i]);
					nbMap.clear();					
					for(int j = 0; j < wdCount; j++) {					
						if(i == j) 
							continue; 
						Text neighbor = new Text(words[j]); 
						
		                if(nbMap.containsKey(neighbor)){ 
		                   IntWritable count = (IntWritable)nbMap.get(neighbor);
		                   count.set(count.get()+1);
		                }
		                else
		                	nbMap.put(neighbor,new IntWritable(1));		                
					}
					context.write(word, nbMap);
				}
			}
		}
		
	}
	
	// Extending reducer class 
	public static class ReducerClass extends Reducer<Text, MapWritable, Text, MapWritable> {
	    private MapWritable mapArray = new MapWritable();
	    
	    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	    	//Clear array
	    	mapArray.clear();
	    	//Iterate through array and set values
	        for (MapWritable value : values) 
	        	mapArrayAddition(value);	       
	        context.write(key, mapArray);
	    }
	    
//	    Setting Array for word using strip approach
	    private void mapArrayAddition(MapWritable nbMap) {
	        Set<Writable> keys = nbMap.keySet();
	        for (Writable key : keys) {
	            IntWritable Sum = (IntWritable) nbMap.get(key);            
	            if (mapArray.containsKey(key)) {
	                IntWritable wordCount = (IntWritable) mapArray.get(key);
	                wordCount.set(wordCount.get() + Sum.get());
	            } 
	            else 
	            	mapArray.put(key, Sum);
	        }
	    }
	}	
	
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Strip Approach");
        
        job.setJarByClass(T1P2.class);
        
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);}
}
