package edu.appstate.kepplemr.counts;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CountSorter -> sorts the output of the various Counts.
 * 
 * @author  michael
 * @version 1.1
 * @since   12 Mar 2014
*/
public class CountsSorter 
{
	private static final Logger log = LoggerFactory.getLogger(Counts.class);
	
    /**
     * Creates and runs the job for creating the reversed index
     * 
     * @param input - input directory
     * @param output - output directory
     * 
     * @throws IOException - error on input or output path I/O.
    */
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public static void runJob(String input, String output) 
    	throws IOException
    {
    	int numReduceTasks = 1;
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "DictionarySorter");
        job.setJarByClass(CountsSorter.class);
        job.setMapperClass(CountSorterMapper.class);
        job.setReducerClass(CountSorterReducer.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setNumReduceTasks(numReduceTasks);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setSortComparatorClass(SortKeyComparator.class);
        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, new Path(output));
        // Randomly sample the data in attempt to evenly balance reduce() load
        // if numReduceTasks is modified.
        job.setPartitionerClass(TotalOrderPartitioner.class);
        Path inputDir = new Path("partition/");
        Path partitionFile = new Path(inputDir, "partitioning");
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
        double pcnt = 10.0;
        int numSamples = numReduceTasks;
        int maxSplits = numReduceTasks - 1;
        if (0 >= maxSplits)
            maxSplits = Integer.MAX_VALUE;
        try 
        {
            InputSampler.Sampler sampler = new InputSampler.RandomSampler(pcnt, numSamples, maxSplits);
            InputSampler.writePartitionFile(job, sampler);
            job.waitForCompletion(true);
        } 
        catch (Exception ex) 
        {
            log.error("Exception: ", ex);
        }
    }
    
    /**
     * CountSorterMapper - attains the Text key and LongWritable value from the Text
     *   file and reverses them so that the Shuffle handlers the sorting. 
    */
    public static class CountSorterMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
    {
        @Override
        protected void map(LongWritable key, Text value, Context context)
        	throws IOException, InterruptedException 
        {
        	String[] line = value.toString().split("\t");
        	Text word = new Text(line[0]);
        	LongWritable convertMe = new LongWritable(Long.parseLong(line[1]));
        	context.write(convertMe, word);
        }
    }
    
    /**
     * CountSorterReducer - Receives the sorted input from the Shuffle, reverses the
     *   pair again then outputs.
    */
    public static class CountSorterReducer extends Reducer<LongWritable, Text, Text, LongWritable> 
    {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> value, Context context)
        	throws IOException, InterruptedException 
        {
        	for (Text val : value)
        		context.write(val, key);
        }
    }
    
    /**
     * Custom Comparator class - default will sort in increasing order.
    */
    public static class SortKeyComparator extends WritableComparator 
    {
        protected SortKeyComparator() 
        {
            super(LongWritable.class, true);
        }
     
        @SuppressWarnings("rawtypes")
		@Override
        public int compare(WritableComparable a, WritableComparable b) 
        {
            LongWritable o1 = (LongWritable) a;
            LongWritable o2 = (LongWritable) b;
            if(o1.get() < o2.get())
                return 1;
            else if (o1.get() > o2.get())
                return -1;
            else
                return 0;
        }
    }
}
