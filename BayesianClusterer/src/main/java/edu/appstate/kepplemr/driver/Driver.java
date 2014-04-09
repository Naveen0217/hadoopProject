package edu.appstate.kepplemr.driver;
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
import org.apache.mahout.text.SequenceFilesFromDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver 
{
	private static final Logger log = LoggerFactory.getLogger(Driver.class);
	
	public static void main(String[] args)
	{
		Configuration.addDefaultResource("/etc/hadoop/conf/hdfs-site.xml");
		Configuration.addDefaultResource("/etc/hadoop/conf/mapred-site.xml");
		Configuration.addDefaultResource("/etc/hadoop/conf/yarn-site.xml");
		Configuration.addDefaultResource("/etc/hadoop/conf/core-site.xml");
		Configuration conf = new Configuration();
		// When packaging with maven-assembly, included JARs can overwrite the proper conf settings; reset them. 
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    Option input = obuilder.withLongName("input").withRequired(true).withArgument(abuilder.withName("<input>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("text document directory to cluster")
	    	.withShortName("i").create();
	    Option output = obuilder.withLongName("output").withRequired(true).withArgument(abuilder.withName("<outputDir>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("output directory for the various stages")
	    	.withShortName("o").create();
	    Group group = gbuilder.withName("Options").withOption(input).withOption(output).create();
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
	    String[] test = { "-i", inputDir, "-o", outputDir };
	    //this.username = cmdLine.getValue(mysqlName).toString();
	    //this.table = cmdLine.getValue(table).toString();
	    //this.key = cmdLine.getValue(tableKey).toString();
	    //this.columns = cmdLine.getValue(columns).toString();
	    
	    
	    try
	    {
	    	SequenceFilesFromDirectory.main(test);
	    }
	    catch (Exception ex)
	    {
	    	System.err.println("Exception -> " + ex.toString());
	    }
		
	}
}
