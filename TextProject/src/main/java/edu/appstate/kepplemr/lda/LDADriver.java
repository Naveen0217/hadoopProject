package edu.appstate.kepplemr.lda;
import java.util.List;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.lda.cvb.CVB0Driver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.utils.vectors.RowIdJob;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.HighDFWordsPruner;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.appstate.kepplemr.main.TextUtils;
import edu.appstate.kepplemr.customanalyzer.GutenbergAnalyzer;

public class LDADriver extends Configured implements Tool
{
	private static final Logger log = LoggerFactory.getLogger(LDADriver.class);
	private static String output;
	private static String input;
	private static int maxFreq;
	private static int minFreq;
	private static int size;
	private static int k;
	private static boolean generateSeqs;
	private static boolean generateSparse;
	private static boolean generateMatrix;
	private static boolean runLDA;
	
	public static void main(String[] args)
	{
		// Must pre-mkdir HDFS output dirs.
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    Option inputOp = obuilder.withLongName("input").withRequired(true).withArgument(abuilder.withName("<input>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("text document directory to cluster")
	    	.withShortName("i").create();
	    Option outputOp = obuilder.withLongName("output").withRequired(true).withArgument(abuilder.withName("<outputDir>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("output directory for the various stages")
	    	.withShortName("o").create();
	    Option maxDFpercent = obuilder.withLongName("maxDFpercent").withRequired(false)
	    	.withArgument(abuilder.withName("<maxPercent>").withMinimum(1).withMaximum(1).create())
	    	.withDescription("maximum document frequency allowed for terms").withShortName("maxDf").create();
	    Option minDFpercent = obuilder.withLongName("minDFpercent").withRequired(false)
		    .withArgument(abuilder.withName("<minPercent>").withMinimum(1).withMaximum(1).create())
		    .withDescription("minimum document frequency allowed for terms").withShortName("minDf").create();
	    Option kValue = obuilder.withLongName("k-value").withRequired(true)
	    	.withArgument(abuilder.withName("<num_topics>").withMinimum(1).withMaximum(1).create())
	    	.withDescription("number of topics to search for").withShortName("k").create();
	    Option skipSeqDir = obuilder.withLongName("skipSeqDir").withRequired(false)
	    	.withDescription("skip SequenceFile generation stage").withShortName("s1").create();
	    Option skipSparse = obuilder.withLongName("skipSparse").withRequired(false)
		    .withDescription("skip SparseVector generation stage").withShortName("s2").create();
	    Option skipMatrix = obuilder.withLongName("skipMatrix").withRequired(false)
			.withDescription("skip SparseMatrix generation stage").withShortName("s3").create();
	    Option skipLda = obuilder.withLongName("skipLda").withRequired(false)
			.withDescription("skip LDA analysis stage").withShortName("s4").create();
	    Option dictSize = obuilder.withLongName("dict Size").withRequired(true)
			.withArgument(abuilder.withName("<# words>").withMinimum(1).withMaximum(1).create())
			.withDescription("number of words present in all dictionaries").withShortName("size").create();
	    Group group = gbuilder.withName("Options").withOption(inputOp).withOption(outputOp).withOption(maxDFpercent)
	    	.withOption(minDFpercent).withOption(skipSeqDir).withOption(skipSparse).withOption(skipMatrix)
	    	.withOption(skipLda).withOption(kValue).withOption(dictSize).create();
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
	    k = Integer.parseInt(cmdLine.getValue(kValue).toString());
	    input = cmdLine.getValue(inputOp).toString();
	    output = cmdLine.getValue(outputOp).toString();
	    generateSeqs = !cmdLine.hasOption(skipSeqDir);
	    generateSparse = !cmdLine.hasOption(skipSparse);
	    generateMatrix = !cmdLine.hasOption(skipMatrix);
	    runLDA = !cmdLine.hasOption(skipLda);
	    size = Integer.parseInt(cmdLine.getValue(dictSize).toString());
	    if (cmdLine.hasOption(maxDFpercent))
	    	maxFreq = Integer.parseInt(cmdLine.getValue(maxDFpercent).toString());
	    else
	    	maxFreq = 50;
	    if (cmdLine.hasOption(minDFpercent))
	    	minFreq = Integer.parseInt(cmdLine.getValue(minDFpercent).toString());
	    else
	    	minFreq = 0;
	    //new Driver(inputDir, outputDir, minFreq, maxFreq);
	    try 
	    {
			ToolRunner.run(new LDADriver(), args);
		} 
	    catch (Exception e) 
	    {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}

	// LogNorm set to true, bigrams
	public int run(String[] args) throws Exception 
	{
		String[] arguments = { "-i", "", "-o", "" };
		if (generateSeqs)
		{
			SequenceFilesFromDirectory sf = new SequenceFilesFromDirectory();
		    arguments[1] = input;
		    arguments[3] = output + "sequenceFiles/";
		    try
		    {
		    	sf.run(arguments);
		    }
		    catch (Exception ex)
		    {
		    	System.err.println("Exception creating SequenceFiles -> " + ex.toString());
		    	ex.printStackTrace();
		    }
		}
		if (generateSparse)
		{
		    // Create SparseVectors from SequenceFile Directory
		    boolean logNormalize = true;
		    boolean seqVects = true;
		    boolean namedVects = true;
		    float minLLR = LLRReducer.DEFAULT_MIN_LLR;
		    float norm = PartialVectorMerger.NO_NORMALIZING;
		    //float norm = Float.
		    int gramSize = 2;
		    int reduceTasks = 1;
		    int chunkSize = 64; 
		    Path inputDir = new Path(output + "sequenceFiles/");
		    Path outputDir = new Path(output + "sparseVectors/");
		    Configuration conf = getConf();
			conf.addResource(new Path("file:///etc/hadoop/conf/hdfs-site.xml"));
			conf.addResource(new Path("file:///etc/hadoop/conf/mapred-site.xml"));
			conf.addResource(new Path("file:///etc/hadoop/conf/yarn-site.xml"));
			conf.addResource(new Path("file:///etc/hadoop/conf/core-site.xml"));
			conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		    Path tokenizedPath = new Path(output, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
		    try 
		    {
		    	// Create TF Vectors
				DocumentProcessor.tokenizeDocuments(inputDir, GutenbergAnalyzer.class, tokenizedPath, conf);
				String tfDirName = DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-toprune";
		        DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,outputDir,
		        	tfDirName, conf, 2, gramSize, minLLR, norm, logNormalize, reduceTasks, chunkSize, seqVects, namedVects);
		        Pair<Long[], List<Path>> docFrequenciesFeatures = null;
		        docFrequenciesFeatures = TFIDFConverter.calculateDF(new Path(outputDir, tfDirName), outputDir, conf, chunkSize);
			    long vectorCount = docFrequenciesFeatures.getFirst()[1];
			    long maxDFThreshold = (long) (vectorCount * (maxFreq / 100.0f));
		        Path tfDir = new Path(outputDir, tfDirName);
		        Path prunedTFDir = new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
		        Path prunedPartialTFDir = new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-partial");
		        HighDFWordsPruner.pruneVectors(tfDir, prunedTFDir, prunedPartialTFDir, maxDFThreshold, minFreq, conf,
		        	docFrequenciesFeatures, norm, logNormalize, reduceTasks);
		        HadoopUtil.delete(new Configuration(conf), tfDir);
			} 
		    catch (Exception ex) 
		    {
		    	System.err.println("Error while generating sparse vectors -> " + ex);
				ex.printStackTrace();
			} 			
		}
		if (generateMatrix)
		{
		    // Create SparseMatrix
		    arguments[1] = output + "sparseVectors/tf-vectors/";
		    arguments[3] = output + "sparseMatrix/";
		    try 
		    {
				ToolRunner.run(new RowIdJob(), arguments);
			} 
		    catch (Exception e) 
		    {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (runLDA)
		{
		    // Run CVB/LDA...
		    arguments = new String[12];
		    arguments[0] = "-i";
		    arguments[1] = output + "sparseMatrix/matrix";
		    arguments[2] = "-o";
		    arguments[3] = output + "cvbOut/";
		    // Works only with fully-qualified argument names.
		    arguments[4] = "--maxIter";
		    arguments[5] = "30";
		    arguments[6] = "--num_topics";
		    arguments[7] = new Integer(k).toString();
		    arguments[8] = "-nt";
		    // 3274483 + 2721162 = 5995645
		    //arguments[9] = "5995645";
		    arguments[9] = new Integer(size).toString();
		    //arguments[9] = 198123;
		    arguments[10] = "-dict";
		    arguments[11] = output + "sparseVectors/dictionary.file-*";
		    // Smoothing?
		    try 
		    {
				CVB0Driver.main(arguments);
			} 
		    catch (Exception e) 
		    {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return 0;
	}
}
