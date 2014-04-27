package edu.appstate.kepplemr.counts;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount
{
	public static class CountsMapper extends Mapper<Object, Text, Text, IntWritable> 
	{
		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			String wordToSearch = conf.get("wordToSearch").toLowerCase();
			word.set(wordToSearch);
			String[] words = value.toString().toLowerCase().split("[,. \"]");
			for (int i = 0; i < words.length; i++)
				if (words[i].compareTo(wordToSearch) == 0)
						context.write(word, ONE);
		}
	}
	
	/**
	 * CountsReducer -> simple reducer that sums the counts output from the
	 *  Mapper.
	*/
	public static class CountsReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable num : values)
				sum += num.get();
			context.write(key, new IntWritable(sum));
		}
	}
}
