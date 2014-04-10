package edu.appstate.kepplemr.driver;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.utils.vectors.RowIdJob;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.apache.mahout.clustering.lda.cvb.CVB0Driver;

public class Driver 
{
	private static final Logger log = LoggerFactory.getLogger(Driver.class);
	
	public static void main(String[] args)
	{		
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    Option input = obuilder.withLongName("input").withRequired(true).withArgument(abuilder.withName("<input>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("text document directory to cluster")
	    	.withShortName("i").create();
	    Option output = obuilder.withLongName("output").withRequired(true).withArgument(abuilder.withName("<outputDir>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("output directory for the various stages")
	    	.withShortName("o").create();
	    Option maxDFpercent = obuilder.withLongName("maxDFpercent").withRequired(false)
	    	.withArgument(abuilder.withName("<maxPercent>").withMinimum(1).withMaximum(1).create())
	    	.withDescription("maximum document frequency allowed for terms").withShortName("maxDf").create();
	    Option minDFpercent = obuilder.withLongName("minDFpercent").withRequired(false)
		    .withArgument(abuilder.withName("<minPercent>").withMinimum(1).withMaximum(1).create())
		    .withDescription("minimum document frequency allowed for terms").withShortName("minDf").create();
	    Group group = gbuilder.withName("Options").withOption(input).withOption(output).withOption(maxDFpercent)
	    	.withOption(minDFpercent).create();
	    Parser parser = new Parser();
	    parser.setGroup(group);
	    CommandLine cmdLine = null;
	    try 
	    {
			cmdLine = parser.parse(args);
		} 
	    catch (OptionException ex) 
	    {
	    	log.debug("Exception: ", ex);
	        TextUtils.printHelp(group);
	        System.exit(-1);
		}
	    String inputDir = cmdLine.getValue(input).toString();
	    String outputDir = cmdLine.getValue(output).toString();
		Integer maxFreq, minFreq;
	    if (cmdLine.hasOption(maxDFpercent))
	    	maxFreq = Integer.parseInt(cmdLine.getValue(maxDFpercent).toString());
	    else
	    	maxFreq = 50;
	    if (cmdLine.hasOption(minDFpercent))
	    	minFreq = Integer.parseInt(cmdLine.getValue(minDFpercent).toString());
	    else
	    	minFreq = 5;
	    
	    String[] arguments = new String[4];
	    arguments[0]= "-i";
	    arguments[1] = inputDir;
	    arguments[2] = "-o";
	    arguments[3] = outputDir + "sequenceFiles/";
	    try
	    {
	    	// Use my modified seqdirectory class with conf bug fixed.
	    	//org.apache.mahout.fixes.SequenceFilesFromDirectory.main(arguments);
	    	SequenceFilesFromDirectory.main(arguments);
	    }
	    catch (Exception ex)
	    {
	    	System.err.println("Exception -> " + ex.toString());
	    }
	    arguments = new String[14];
	    arguments[0] = "-i";
	    arguments[1] = outputDir + "sequenceFiles/";
	    arguments[2] = "-o";
	    arguments[3] = outputDir + "sparseVectors/";
	    arguments[4] = "-x";
	    arguments[5] = maxFreq.toString();
	    arguments[6] = "-md";
	    arguments[7] = minFreq.toString();
	    arguments[8] = "-seq";
	    arguments[9] = "--namedVector";
	    arguments[10] = "-wt";
	    arguments[11] = "tf";
	    arguments[12] = "-a";
	    arguments[13] = "edu.appstate.kepplemr.customanalyzer.GutenbergAnalyzer";
	    try
	    {
	    	//org.apache.mahout.fixes.SparseVectorsFromSequenceFiles.main(arguments);
	    	SparseVectorsFromSequenceFiles.main(arguments);
	    }
	    catch (Exception ex)
	    {
	    	System.err.println("Exception -> " + ex.toString());
	    	ex.printStackTrace();
	    }
	    arguments = new String[4];
	    arguments[0] = "-i";
	    arguments[1] = outputDir + "sparseVectors/tf-vectors/";
	    arguments[2] = "-o";
	    arguments[3] = outputDir + "sparseMatrix/";
	    try
	    {
	    	RowIdJob.main(arguments);
	    }
	    catch (Exception ex)
	    {
	    	System.err.println("Exception -> " + ex.toString());
	    	ex.printStackTrace();
	    }
	    arguments = new String[20];
	    arguments[0] = "-i";
	    arguments[1] = outputDir + "sparseMatrix/";
	    // TBC
	    
	}
}
