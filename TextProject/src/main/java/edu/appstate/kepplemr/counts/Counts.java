package edu.appstate.kepplemr.counts;
import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.appstate.kepplemr.main.TextUtils;

/**
 * Counts -> analyzes Sqoop-outputted SequenceFile, computes various counts for
 *   user-inputted word.
 * 
 * @author  michael
 * @version 1.1
 * @since   16 Mar 2014
*/
public class Counts 
{
	/**
	 * countType - determines the counting operation to be performed with respect
	 *   to the specified word - unigram analysis, bigram analysis, or total sentence
	 *   cooccurence. 
	*/
	private enum countType { UNIGRAM, LEFTNEIGHBOR, RIGHTNEIGHBOR, COOCCURENCES, NONE }
	private static final Logger log = LoggerFactory.getLogger(Counts.class);
	
	/**
	 *  Main method - processes CLI flags and sets up specified counts. 
	 *  @param args Command Line arguments.
	 *  
	 *  @throws URISyntaxException 
	 *  @throws IOException 
	*/
	public static void main(String[] args) throws IOException, URISyntaxException
	{
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    Option theWord = obuilder.withLongName("word").withRequired(true).withArgument(abuilder.withName("word")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Word to count").withShortName("w").create();
		Option input = obuilder.withLongName("input").withRequired(true).withArgument(abuilder.withName("input")
			.withMinimum(1).withMaximum(1).create()).withDescription("SequenceFile input directory")
			.withShortName("i").create();
		Option output = obuilder.withLongName("output").withRequired(true).withArgument(abuilder.withName("output")
			.withMinimum(1).withMaximum(1).create()).withDescription("Output directory")
			.withShortName("o").create();
		Option unigramCount = obuilder.withLongName("leftBigramCount").withRequired(false)
				.withDescription("Generate left neighbor count").withShortName("u").create();
		Option leftNeighbors = obuilder.withLongName("leftBigramCount").withRequired(false)
			.withDescription("Generate left neighbor count").withShortName("l").create();
		Option rightNeighbors = obuilder.withLongName("rightBigramCount").withRequired(false)
			.withDescription("Generate right neighbor count").withShortName("r").create();
		Option cooccurences = obuilder.withLongName("cooccurenceCount").withRequired(false)
			.withDescription("Generate sentence co-occurence count").withShortName("c").create();
	    Group group = gbuilder.withName("Options").withOption(theWord).withOption(input).withOption(output)
	    	.withOption(leftNeighbors).withOption(rightNeighbors).withOption(cooccurences).withOption(unigramCount).create();
		Parser parser = new Parser();
		parser.setGroup(group);
		CommandLine cmdLine = null;
	    try 
	    {
			cmdLine = parser.parse(args);
		} 
	    catch (OptionException ex) 
	    {
	    	log.error("Exception: ", ex);
	        TextUtils.printHelp(group);
	        System.exit(-1);
		}    
		Configuration conf = new Configuration();
		// When packaging with maven-assembly, included JARs can overwrite the proper conf settings; reset them. 
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("mapreduce.reduce.log.level", "ERROR");
		conf.set("mapreduce.map.log.level", "ERROR");
		conf.set("wordToSearch", cmdLine.getValue(theWord).toString());
		conf.set("inputDir", cmdLine.getValue(input).toString());
		String outputDir = cmdLine.getValue(output).toString();
    	if (cmdLine.hasOption(unigramCount))
    	{
        	conf.set("outputDir", outputDir + "/count/");
        	conf.setEnum("countType", countType.UNIGRAM);
        	deployJob(conf);
    	}
	    if (cmdLine.hasOption(leftNeighbors))
	    {
	    	conf.set("outputDir", cmdLine.getValue(output).toString() + "/leftNeighbors/");
	    	conf.setEnum("countType", countType.LEFTNEIGHBOR);
	    	deployJob(conf);
        	String inputs = outputDir + "/leftNeighbors/*";
        	String outputs = "outputSort/" + "leftNeighbors/";
        	CountsSorter.runJob(inputs, outputs);
	    }
	    if (cmdLine.hasOption(rightNeighbors))
	    {
	    	conf.set("outputDir", cmdLine.getValue(output).toString() + "/rightNeighbors/");
	    	conf.setEnum("countType", countType.RIGHTNEIGHBOR);
	    	deployJob(conf);
        	String inputs = outputDir + "/rightNeighbors/*";
        	String outputs = "outputSort/" + "rightNeighbors/";
        	CountsSorter.runJob(inputs, outputs);
	    }
	    if (cmdLine.hasOption(cooccurences))
	    {
	    	conf.set("outputDir", cmdLine.getValue(output).toString() + "/cooccurences/");
	    	conf.setEnum("countType", countType.COOCCURENCES);
	    	deployJob(conf);
        	String inputs = outputDir + "/cooccurences/*";
        	String outputs = "outputSort/" + "cooccurences/";
        	CountsSorter.runJob(inputs, outputs);
	    }
	}
	
	/**
	 * deployJob - deploys MapReduce count job with specified Configuration.
	 * 
	 * @param conf Configuration values for the count, specifying specific output
	 *   directories and count type. 
	*/
	public static void deployJob(Configuration conf)
	{	
		try
		{
			Job job = Job.getInstance(conf);
			job.setJarByClass(CountsMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);
			job.setMapperClass(CountsMapper.class);
			job.setReducerClass(CountsReducer.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(conf.get("inputDir")));
			FileOutputFormat.setOutputPath(job, new Path(conf.get("outputDir")));
			job.waitForCompletion(true);
		}
		catch (Exception ex)
		{
	    	log.debug("Exception: ", ex);
	        System.exit(-1);
		}
	}
	
	/**
	 * CountsMapper -> 	Mapper that looks for specified word and based upon
	 *   countType enum will generate the correct counts for the varying options
	 *   (left neighbors, right neighbors, unigram, sentence coocccurence).
	*/
	public static class CountsMapper extends Mapper<Text, NullWritable, Text, LongWritable> 
	{
		private final static LongWritable ONE = new LongWritable(1L);
		private Text word = new Text();
		
		public void map(Text key, NullWritable value, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			String wordToSearch = conf.get("wordToSearch");
			word.set(wordToSearch);
			String[] words = key.toString().split("[,. \"]");
			for (int i = 0; i < words.length; i++)
			{
				if (words[i].compareTo(wordToSearch) == 0)
				{
					
					if (conf.getEnum("countType", countType.NONE) == countType.UNIGRAM)
						context.write(word, ONE);
					else if (conf.getEnum("countType", countType.NONE) == countType.LEFTNEIGHBOR)
						context.write(new Text(words[i-1]), ONE);
					else if (conf.getEnum("countType", countType.NONE) == countType.RIGHTNEIGHBOR)
						context.write(new Text(words[i+1]), ONE);
					else if (conf.getEnum("countType", countType.NONE) == countType.COOCCURENCES)
					{
						for (String word : words)
							context.write(new Text(word), ONE);
						break;
					}
				}
			}
		}
	}

	/**
	 * CountsReducer -> simple reducer that sums the counts output from the
	 *  Mapper.
	*/
	public static class CountsReducer extends Reducer<Text, LongWritable, Text, LongWritable> 
	{
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException 
		{
			long sum = 0L;
			for (LongWritable num : values)
				sum += num.get();
			context.write(key, new LongWritable(sum));
		}
	}
}
