package edu.appstate.kepplemr.configprint;
import java.util.Map.Entry;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.*;

public class ConfigPrinter extends Configured implements Tool 
{
	public int run(String[] args) throws Exception 
	{
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("yarn-default.xml");
		Configuration conf = getConf();
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		for (Entry<String, String> entry : conf)
			System.out.printf("%s=%s\n", entry.getKey(), entry.getValue());
		System.out.println("*** Deprecated keys: ***");
		Configuration.dumpDeprecatedKeys();
		return 0;
	}

	public static void main(String[] args) throws Exception 
	{
		int exitCode = ToolRunner.run(new ConfigPrinter(), args);
		System.exit(exitCode);
	}
}