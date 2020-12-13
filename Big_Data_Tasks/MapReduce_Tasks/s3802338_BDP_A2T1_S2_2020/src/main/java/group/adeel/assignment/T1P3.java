package group.adeel.assignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import group.adeel.assignment.T1P1.WordPair;
import java.io.IOException;

public class T1P3 {
	
	public static class MapperClass extends Mapper<Object, Text, WordPair, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		private WordPair stringPair = new WordPair();
		private IntWritable count = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String cleanText = value.toString().replaceAll("\\s+", " ").trim();
			String[] words = cleanText.split("\\s+"); 
			int wdCount = words.length;
			
			if(wdCount > 1) {
				// outer loop to find for all the words in a line
				for(int i = 0; i< wdCount; i++) {					
					// skip if empty string
					if(words.equals("")) 
						continue;				
					stringPair.setWord(words[i]);					
					// inner loop 
					for(int j = 0; j < wdCount; j++){
						// skip if words are same
						if(i == j) 
							continue; 			
						stringPair.setNeighbor(words[j]);
						context.write(stringPair, one);
					}
					stringPair.setNeighbor("*");
                    count.set(wdCount);
                    context.write(stringPair, count);
				}
			}
		}
	}
	
	//Extending Reducer class
	public static class ReducerClass extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {	    
	    private Text tempWord = new Text("NOT_SET");
	    private Text temp = new Text("*");
	    
	    private DoubleWritable count = new DoubleWritable();
	    private DoubleWritable freq = new DoubleWritable();

	    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	int getValues = 0;
	        
	    	if (key.getNeighbor().equals(temp)) {
	            if (key.getWord().equals(tempWord)) 
	                count.set(count.get() + sumAll(values));           
	            else {
	                tempWord.set(key.getWord());
	                count.set(0);
	                count.set(sumAll(values));
	            }
	        } 
	        else {
	            getValues = sumAll(values);
	            freq.set((double) getValues / count.get());
	            context.write(key, freq);
	        }
	    }
	    
	    // Sum all values 
	    private int sumAll(Iterable<IntWritable> values) {
			int count = 0;
			for (IntWritable value : values) 
			    count = count + value.get();
			return count;
	    }
	}
	
	// Extending Partitioner class
	public static class PairPartitioner extends Partitioner<WordPair,IntWritable> {
	    @Override
	    public int getPartition(WordPair stringPair, IntWritable intWritable, int numPartitions) {
	        return Math.abs(stringPair.getWord().hashCode()) % numPartitions;
	    }
	}
	
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Word Relative Frequecies Pair Approach");
	    
	    job.setJarByClass(T1P3.class);
	    job.setNumReduceTasks(3);
	    
	    job.setMapperClass(MapperClass.class);
	    job.setPartitionerClass(PairPartitioner.class);
	    job.setReducerClass(ReducerClass.class);
	    
	    job.setOutputKeyClass(WordPair.class);
	    job.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);}
}
