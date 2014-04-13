package edu.appstate.kepplemr.nullseqdump;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.appstate.kepplemr.main.TextUtils;

/**
 * SeqDump -> displays SequenceFile (can contain null, NullWritable).
 * 
 * @author  michael
 * @version 1.1
 * @since   08 Mar 2014
*/
public class SeqDump 
{
	private static final Logger log = LoggerFactory.getLogger(SeqDump.class);
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException 
	{
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    Option input = obuilder.withLongName("input").withRequired(true).withArgument(abuilder.withName("input")
		    .withMinimum(1).withMaximum(1).create()).withDescription("Input directory for SequenceFiles")
		    .withShortName("i").create();
	    Group group = gbuilder.withName("Options").withOption(input).create();
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
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		// When packaging with maven-assembly, included JARs can overwrite the proper conf settings; reset them. 
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(cmdLine.getValue(input).toString());
		SequenceFile.Reader reader = null;
		try 
		{
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) 
			{
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key,value);
				position = reader.getPosition();
			}
		} 
		finally 
		{
			IOUtils.closeStream(reader);
		}
	}
}
