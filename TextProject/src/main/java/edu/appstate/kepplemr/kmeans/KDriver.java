package edu.appstate.kepplemr.kmeans;
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
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.HighDFWordsPruner;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.appstate.kepplemr.customanalyzer.GutenbergAnalyzer;
import edu.appstate.kepplemr.main.TextUtils;

// mahout kmeans -i /user/michael/sparsevectors/tfidf-vectors/ -c initial-clusters -o kmeans-clusters 
// -dm org.apache.mahout.common.distance.CosineDistanceMeasure -cd 0.05 -k 2 -x 20 -cl

// %%%%%%%%%%%%%%%%
//sudo java -jar Sql2Seq-0.0.1-jar-with-dependencies.jar
//mahout seq2sparse -i /user/michael/sequencefiles/* -o sparsevectors -ow
//mahout kmeans -i /user/michael/sparsevectors/tfidf-vectors/ -c initial-clusters -o kmeans-clusters -dm org.apache.mahout.common.distance.CosineDistanceMeasure -cd 0.05 -k 2 -x 20 -cl
//mahout clusterdump -dt sequencefile -d hdfs://localhost:8020/user/michael/sparsevectors/dictionary.file-0 --input hdfs://localhost:8020/user/michael/kmeans-clusters/clusters-3-final --output /home/michael/test.txt -b 10 -n 10

public class KDriver extends Configured implements Tool
{
	private static final Logger log = LoggerFactory.getLogger(KDriver.class);
	private static int k;
	private static Path clusters;
	private static boolean generateSeqs;
	private static boolean generateSparse;
	private static boolean generateKMeans;
	private static String output;
	private static String input;
	private static int maxIter;
	private static int maxFreq;
	private static int minFreq;
	
	public static void main(String[] args) throws Exception
	{
		ToolRunner.run(new Configuration(), new KDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception 
	{
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
	    Option maxIterations = obuilder.withLongName("maxIter").withRequired(false)
			.withArgument(abuilder.withName("<# iterations>").withMinimum(1).withMaximum(1).create())
			.withDescription("maximum clustering iterations to perform").withShortName("m").create();
	    Option skipSeqDir = obuilder.withLongName("skipSeqDir").withRequired(false)
	    	.withDescription("skip SequenceFile generation stage").withShortName("s1").create();
	    Option skipSparse = obuilder.withLongName("skipSparse").withRequired(false)
			.withDescription("skip SparseVector generation stage").withShortName("s2").create();
	    Option skipCluster = obuilder.withLongName("skipClustering").withRequired(false)
			.withDescription("skip KMeans clustering stage").withShortName("s3").create();
	    Group group = gbuilder.withName("Options").withOption(kValue).withOption(inputOp).withOption(outputOp)
	    	.withOption(maxDFpercent).withOption(minDFpercent).withOption(skipSeqDir).withOption(skipSparse)
	    	.withOption(skipCluster).withOption(maxIterations).create();
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
	    maxIter = Integer.parseInt(cmdLine.getValue(maxIterations).toString());
	    input = cmdLine.getValue(inputOp).toString();
	    output = cmdLine.getValue(outputOp).toString();
	    generateSeqs = !cmdLine.hasOption(skipSeqDir);
	    generateSparse = !cmdLine.hasOption(skipSparse);
	    generateKMeans = !cmdLine.hasOption(skipCluster);
	    if (cmdLine.hasOption(maxDFpercent))
	    	maxFreq = Integer.parseInt(cmdLine.getValue(maxDFpercent).toString());
	    else
	    	maxFreq = 50;
	    if (cmdLine.hasOption(minDFpercent))
	    	minFreq = Integer.parseInt(cmdLine.getValue(minDFpercent).toString());
	    else
	    	minFreq = 0;
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
			Path inputDir;
			if (!generateSeqs)
				inputDir = new Path(input);
			else
				inputDir = new Path(output + "sequenceFiles/");
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
				// TF-IDF
				String tfDirName = DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-toprune";
				DocumentProcessor.tokenizeDocuments(inputDir, GutenbergAnalyzer.class, tokenizedPath, conf);
				DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath, outputDir, tfDirName, conf, 2, 
					gramSize, minLLR, -1.0f, false, reduceTasks, chunkSize, seqVects, namedVects);
				Pair<Long[], List<Path>> docFrequenciesFeatures = null;
				docFrequenciesFeatures = TFIDFConverter.calculateDF(new Path(outputDir, tfDirName), outputDir, conf, chunkSize);
				long vectorCount = docFrequenciesFeatures.getFirst()[1];
				long maxDFThreshold = (long) (vectorCount * (maxFreq / 100.0f));
		        Path tfDir = new Path(outputDir, tfDirName);
		        Path prunedTFDir = new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
		        Path prunedPartialTFDir = new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-partial");
		        HighDFWordsPruner.pruneVectors(tfDir, prunedTFDir, prunedPartialTFDir, maxDFThreshold, minFreq, conf, 
		        	docFrequenciesFeatures, -1.0f, false, reduceTasks);
		        HadoopUtil.delete(new Configuration(conf), tfDir);
		        TFIDFConverter.processTfIdf(new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER), outputDir, 
		        	conf, docFrequenciesFeatures, minFreq, maxFreq, norm, logNormalize, seqVects, namedVects, reduceTasks);
			} 
			catch (Exception ex) 
			{
				System.err.println("Error while generating sparse vectors -> " + ex);
				ex.printStackTrace();
			} 			
		}
		if (generateKMeans)
		{
			input = output + "sparseVectors/tfidf-vectors";
			output = output + "kmeans";
			Path clusters = new Path("clusters");
			double convergenceDelta = 0;
			String measureClass = "org.apache.mahout.common.distance.CosineDistanceMeasure";
			DistanceMeasure measure = ClassUtils.instantiateAs(measureClass, DistanceMeasure.class);
		    clusters = RandomSeedGenerator.buildRandom(getConf(), new Path(input), clusters, k, measure);
		    KMeansDriver.run(getConf(), new Path(input), clusters, new Path(output), convergenceDelta, maxIter, true, 0.0, false);
		}
		return 0;
	}
}
