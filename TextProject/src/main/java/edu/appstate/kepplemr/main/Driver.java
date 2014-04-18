package edu.appstate.kepplemr.main;
import org.apache.log4j.LogManager;
import edu.appstate.kepplemr.autosqoop.AutoSqoop;
import edu.appstate.kepplemr.counts.Counts;
import edu.appstate.kepplemr.hadoopconfig.ConfigPrinter;
import edu.appstate.kepplemr.nullseqdump.SeqDump;
import edu.appstate.kepplemr.sql2seq.SQL2Seq;

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
					edu.appstate.kepplemr.lda.LDADriver.main(arguments);
			}
		}
		catch (Exception ex) {}
	}
}
