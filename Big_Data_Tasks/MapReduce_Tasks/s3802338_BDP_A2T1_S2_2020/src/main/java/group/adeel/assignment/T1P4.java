package group.adeel.assignment;

import org.apache.hadoop.conf.Configuration;
import java.util.Set;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class T1P4 {
	
	public static class MapperClass extends Mapper<Object, Text, Text,MapWritable> {
		private Text word = new Text();
		private MapWritable nbMap = new MapWritable(); 
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String cleanText = value.toString().replaceAll("\\s+", " ").trim();
			String[] allWords = cleanText.split("\\s+"); 
			int wdCount = allWords.length;
			
			// if there are more than one words in our line then iterate through loop
			if(wdCount > 1) {
				//outer loop
				for(int i=0; i<wdCount; i++){
					nbMap.clear();
					word.set(allWords[i]);
					//inner loop
					for(int j=0; j < wdCount; j++) {
						// skip if words are same
						if (i == j) 
							continue;  
						Text neighbor = new Text(allWords[j]); 
		                if(nbMap.containsKey(neighbor)){ 
		                   DoubleWritable wCount = (DoubleWritable)nbMap.get(neighbor);
		                   wCount.set(wCount.get()+1);
		                }
		                else
		                	nbMap.put(neighbor,new DoubleWritable(1));
					}
					context.write(word, nbMap);
				}
			}
		}
	}
	
	// Extending Reducer Class
	public static class ReducerClass extends Reducer<Text, MapWritable, Text, MapWritable> {
	    private MapWritable mapArray = new MapWritable();
	    private MapWritable tempMapArray = new MapWritable();
	    private DoubleWritable sum = new DoubleWritable();
	    private DoubleWritable feq = new DoubleWritable();
	    
	    public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
	    	mapArray.clear();
	    	//mapping 
	        for (MapWritable value : values) 
	        	mapArrayAddition(value);
	        
	        // writing frequencies
	        Set<Writable> keys = mapArray.keySet();
	        for (Writable tempKey : keys) {
	        	DoubleWritable valueKey = (DoubleWritable) mapArray.get(tempKey);
	        	double t = valueKey.get() / sum.get();
	        	feq.set(t);
	        	tempMapArray.put(tempKey, feq);
	        }
	        context.write(key, tempMapArray);
	    }
	    
	    
	    public void mapArrayAddition(MapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        
	        for (Writable key1 : keys) {
	        	DoubleWritable fromCount = (DoubleWritable) mapWritable.get(key1);	    
	            sum.set(sum.get() + (double) fromCount.get());  	     
	            if (mapArray.containsKey(key1)) {
	            	DoubleWritable wCount = (DoubleWritable) mapArray.get(key1);
	                wCount.set(wCount.get() + fromCount.get());
	            } 
	            else
	            	mapArray.put(key1, fromCount);
	        }
	    }
	}
	
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Relative Word Frequency Strip Approach");
        
        job.setJarByClass(T1P4.class);
        
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);}
	
}
