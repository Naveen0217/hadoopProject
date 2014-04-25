package edu.appstate.kepplemr.hivecounts;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Coccurrences extends UDF
{
	private Text result = new Text();
	
	public Text evaluate(Text line, Text word)
	{
		String sentence = line.toString().toLowerCase();
		result.set(sentence.replace(word.toString(), ""));
		return result;
	}
}