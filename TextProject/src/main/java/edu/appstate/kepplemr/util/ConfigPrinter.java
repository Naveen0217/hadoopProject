package edu.appstate.kepplemr.util;
import java.net.URI;
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.util.*;

/**
 * ConfigPrinter -> displays all currently set Hadoop configuration values.
 * 
 * @author  michael
 * @version 1.1
 * @since   08 Mar 2014
*/
public class ConfigPrinter extends Configured implements Tool 
{
	public int run(String[] args) throws Exception 
	{
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("yarn-default.xml");
		Configuration.addDefaultResource("core-default.xml");
		Configuration conf = getConf();
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		FileSystem fs = new RawLocalFileSystem();
		fs.initialize(new URI("/test"), conf);
		for (Entry<String, String> entry : conf)
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		System.out.println("*** Deprecated keys: ***");
		Configuration.dumpDeprecatedKeys();
		fs.close();
		return 0;
	}

	public static void main(String[] args) throws Exception 
	{
		int exitCode = ToolRunner.run(new ConfigPrinter(), args);
		System.exit(exitCode);
	}
}