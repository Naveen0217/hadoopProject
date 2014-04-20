package edu.appstate.kepplemr.util;
import java.io.IOException;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.RandomWriter;
import org.apache.hadoop.examples.Sort;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;

public class Benchmark extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new Configuration(), new Benchmark(), args);
	}

	@Override
	public int run(String[] args) throws Exception 
	{
		//
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    Option input = obuilder.withLongName("input").withRequired(true).withArgument(abuilder.withName("<randomDataDir>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Input directory (defaults to /var/lib/mysql)")
		    .withShortName("i").create();
	    Option output = obuilder.withLongName("output").withRequired(true).withArgument(abuilder.withName("<sortedDataDir>")
		    .withMinimum(1).withMaximum(1).create()).withDescription("Output directory (defaults to /user/$USERNAME/sqoopOut/)")
		    .withShortName("o").create();
	    Option iter = obuilder.withLongName("iterations").withRequired(false).withArgument(abuilder.withName("<iterations")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Number of testing iterations to perform")
	    	.withShortName("x").create();
	    Group group = gbuilder.withName("Options").withOption(input).withOption(output).withOption(iter).create();
		Parser parser = new Parser();
		parser.setGroup(group);
		CommandLine cmdLine = null;
		try 
		{
			cmdLine = parser.parse(args);
		} 
		catch (OptionException e)
		{
			e.printStackTrace();
		}
		int iterations = 3;
	    if (cmdLine.hasOption(iter))
	    	iterations = Integer.parseInt(cmdLine.getValue(iter).toString());
		String inputDir = cmdLine.getValue(input).toString();
	    String outputDir = cmdLine.getValue(output).toString();
	    String[] arguments = { inputDir, outputDir };
	    runTests(arguments, iterations);
		return 0;
	}
	
	public void runTests(String[] arguments, int iterations) throws IOException
	{
		String[] args = { "randomdata" };
		Configuration conf = new Configuration();
		// Compress Map output with Snappy
		conf.set("mapred.compress.map.output","true");
		conf.set("mapred.map.output.compression.codec","org.apache.hadoop.io.compress.SnappyCodec");
		long[] before = new long[iterations];
		long[] middle = new long[iterations];
		long[] after = new long[iterations];
		for (int i = 0; i < iterations; i++)
		{
			HadoopUtil.delete(conf, new Path(arguments[1]));
			before[i] = runSort(arguments, conf);
		}
		// HDFS block size = 128MB
		conf.set("mapred.compress.map.output", "false");
		conf.set("dfs.blocksize", "134217728");
		generateRandom(args);
		for (int i = 0; i < iterations; i++)
		{
			HadoopUtil.delete(conf, new Path(arguments[1]));
			middle[i] = runSort(arguments, conf);
		}
		// HDFS block size = 64MB
		conf.set("dfs.blocksize", "67108864");
		generateRandom(args);
		for (int i = 0; i < iterations; i++)
		{
			HadoopUtil.delete(conf, new Path(arguments[1]));
			after[i] = runSort(arguments, conf);
		}
		// Display Results
		for (int i = 0; i < iterations; i++)
			System.out.println("Before Time -> " + before[i]);
		for (int i = 0; i < iterations; i++)
			System.out.println("Middle Time -> " + middle[i]);
		for (int i = 0; i < iterations; i++)
			System.out.println("After Time -> " + after[i]);
	}
	
	public void generateRandom(String[] args)
	{
		try 
		{
			ToolRunner.run(new Configuration(), new RandomWriter(), args);
		} 
		catch (Exception e) 
		{
			System.err.println("Error generating random data for sort.");
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("rawtypes")
	public long runSort(String[] arguments, Configuration conf)
	{
		final long startTime = System.currentTimeMillis();
		try 
		{
			ToolRunner.run(conf, new Sort(), arguments);
		} 
		catch (Exception ex) 
		{
			System.err.println("Exception during Sort -> " + ex);
			ex.printStackTrace();
		}
		final long endTime = System.currentTimeMillis();
		return endTime - startTime;
	}
}
