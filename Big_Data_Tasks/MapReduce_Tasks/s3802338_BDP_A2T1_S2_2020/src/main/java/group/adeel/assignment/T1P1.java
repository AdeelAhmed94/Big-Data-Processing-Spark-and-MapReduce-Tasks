package group.adeel.assignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.log4j.Logger;

// pairs approach cooccurance matrix

import java.io.*;

public class T1P1 {
	
	public static class WordPair implements Writable, WritableComparable<WordPair> {

	    private Text word;
	    private Text neighbor;

	    public WordPair(Text word, Text neighbor) {
	        this.word = word;
	        this.neighbor = neighbor;
	    }

	    public WordPair(String word, String neighbor) {
	        this(new Text(word),new Text(neighbor));
	    }

	    public WordPair() {
	        this.word = new Text();
	        this.neighbor = new Text();
	    }

	    @Override
	    public int compareTo(WordPair other) {
	        int returnVal = this.word.compareTo(other.getWord());
	        if (returnVal != 0)
	            return returnVal;     
	        if (this.neighbor.toString().equals("*"))
	            return -1;	      
	        else if(other.getNeighbor().toString().equals("*"))
	            return 1;	      
	        return this.neighbor.compareTo(other.getNeighbor());
	    }

	    public WordPair read(DataInput in) throws IOException {
	        WordPair stringPair = new WordPair();
	        stringPair.readFields(in);
	        return stringPair;
	    }

	    @Override
	    public String toString() {
	        return "[word = ("+word+")" + " word neighbor = ("+neighbor+")]";
	    }

	    @Override
	    public int hashCode() {
	        int result = 0;
	        if (word != null)
	        	result = word.hashCode();
	        else
        		result = 0;
	        
	        if (neighbor != null ) 
	        	result = 163 * result + neighbor.hashCode();
        	else
        		result = 163 * result + 0;	
	        
	        return result;
	    }

	    public void setWord(String word){
	        this.word.set(word);
	    }
	    public void setNeighbor(String neighbor){
	        this.neighbor.set(neighbor);
	    }

	    public Text getWord() {
	        return word;
	    }

	    public Text getNeighbor() {
	        return neighbor;
	    }

		@Override
		public void readFields(DataInput arg0) throws IOException {
			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
		}
	}
	
	
	public static class MapperClass extends Mapper<Object, Text, WordPair, IntWritable> {
	
		private WordPair stringPair = new WordPair();
		private static IntWritable one = new IntWritable(1);
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			// Remove multiple white spaces from our text
			String cleanText = value.toString().replaceAll("\\s+", " ").trim();
			String[] words = cleanText.split("\\s+");  
			int wordCount = words.length;
			
			if(wordCount > 1) {
				//Outer loop to loop through the line and get all words
				for(int i=0; i< wordCount; i++) {
					stringPair.setWord(words[i]);
					//Inner loop for all the neighbors for the word
					for(int j = 0; j<wordCount; j++) {
						// skip if words are same
						if(i == j) 
							continue; 
						stringPair.setNeighbor(words[j]);
						context.write(stringPair, one);
					}
				}
			}
		}
	}
	
	//Extending Reducer class
	public static class ReducerClass extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
	    private IntWritable sumCount = new IntWritable();
	    
	    public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {	   
	        int count = 0;	 	   
	        for (IntWritable value : values) 
	        	count = count + value.get();	       
	        
	        sumCount.set(count);
	        context.write(key,sumCount);
	    }
	}

	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Pair Approach");
        
        job.setJarByClass(T1P1.class);
        
        job.setMapperClass(MapperClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
