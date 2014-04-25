package edu.appstate.kepplemr.hivecounts;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class Count extends UDF
{
	private IntWritable result = new IntWritable();
	
	public IntWritable evaluate(Text line, Text word)
	{
		String sentence = line.toString().toLowerCase();
		String token = word.toString().toLowerCase();
		int index = sentence.indexOf(token);
		int count = 0;
		while (index != -1) 
		{
		    count++;
		    sentence = sentence.substring(index + 1);
		    index = sentence.indexOf(token);
		}
		result.set(count);
		return result;
	}
}