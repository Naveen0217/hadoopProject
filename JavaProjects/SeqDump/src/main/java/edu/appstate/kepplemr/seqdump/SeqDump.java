package edu.appstate.kepplemr.seqdump;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SeqDump 
{
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException 
	{
		String uri = args[0];
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		// When packaging with maven-assembly, included JARs can overwrite the proper conf settings; reset them. 
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(uri);
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
