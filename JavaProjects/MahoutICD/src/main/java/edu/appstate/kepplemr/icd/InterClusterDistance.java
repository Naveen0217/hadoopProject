package edu.appstate.kepplemr.icd;
import java.io.Console;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;

/**
 * 
 * @author michael - Much of code found in Ted Dunning's Mahout in Action.
 * 
*/
public class InterClusterDistance 
{
	public static void main(String args[]) throws Exception 
	{
		System.out.println("*** Welcome to InterClusterDistance Analyzer ***");
		Console console = System.console();
		String inputFile = console.readLine("Input file(s): ");
		Configuration conf = new Configuration();
		// When packaging with maven-assembly, included JARs can overwrite the proper conf settings; reset them. 
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		Path path = new Path(inputFile);
		System.out.println("Input Path: " + path);
		List<Cluster> clusters = new ArrayList<Cluster>();
		SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path));
		Writable key = (Writable) reader.getKeyClass().newInstance();
		ClusterWritable value = (ClusterWritable) reader.getValueClass().newInstance();
		while (reader.next(key, value)) 
		{
			Cluster cluster = value.getValue();
			clusters.add(cluster);
			value = (ClusterWritable) reader.getValueClass().newInstance();
		}
		DistanceMeasure measure = new CosineDistanceMeasure();
		double max = 0;
		double min = Double.MAX_VALUE;
		double sum = 0;
		int count = 0;
		for (int i = 0; i < clusters.size(); i++) 
		{
			for (int j = i + 1; j < clusters.size(); j++) 
			{
				double d = measure.distance(((Cluster) clusters.get(i)).getCenter(),((Cluster) clusters.get(j)).getCenter());
				min = Math.min(d, min);
				max = Math.max(d, max);
				sum += d;
				count++;
			}
		}
		System.out.println("Maximum Intercluster Distance: " + max);
		System.out.println("Minimum Intercluster Distance: " + min);
		System.out.println("Average Intercluster Distance(Scaled): "
				+ (sum / count - min) / (max - min));
		reader.close();
	}
}
