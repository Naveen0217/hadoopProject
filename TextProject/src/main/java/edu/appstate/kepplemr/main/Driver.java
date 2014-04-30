package edu.appstate.kepplemr.main;
import org.apache.log4j.LogManager;

import edu.appstate.kepplemr.counts.Counts;
import edu.appstate.kepplemr.dataimport.AutoSqoop;
import edu.appstate.kepplemr.dataimport.SQL2Seq;
import edu.appstate.kepplemr.kmeans.KDriver;
import edu.appstate.kepplemr.lda.LDADriver;
import edu.appstate.kepplemr.util.Benchmark;
import edu.appstate.kepplemr.util.ConfigPrinter;
import edu.appstate.kepplemr.util.SeqDump;

/**
 * Driver -> entry point for TextProject - passes CLI arugments off to 
 *   user-specified operational class.
 * 
 * @author  michael
 * @version 1.1
 * @since   24 Mar 2014
*/
public class Driver 
{
	public String[] arguments;
	
	public static void main(String[] args)
	{
		new Driver(args);
	}
	
	public Driver(String[] args)
	{
		// Reload the Hadoop log4j properties / occasionally un-shut it up.
		LogManager.resetConfiguration();
		String[] arguments = new String[args.length-1];
		System.arraycopy(args, 1, arguments, 0, args.length-1);
		try
		{
			switch(args[0])
			{
				case "autosqoop":
					AutoSqoop.main(arguments);
					break;
				case "counts":
					Counts.main(arguments);
					break;
				case "config":
					ConfigPrinter.main(arguments);
					break;
				case "seqdump":
					SeqDump.main(arguments);
					break;
				case "sql2seq":
					SQL2Seq.main(arguments);
					break;
				case "lda":
					LDADriver.main(arguments);
					break;
				case "benchmark":
					Benchmark.main(arguments);
					break;
				case "kmeans":
					KDriver.main(arguments);
					break;
			}
		}
		catch (Exception ex) {}
	}
}
