package edu.appstate.kepplemr.hivecounts;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class Cooccurrences extends UDF
{
	private Text result = new Text();
	
	public Text evaluate(Text line, Text word)
	{
		result.set("");
		String sentence = line.toString().toLowerCase();
		String token = word.toString().toLowerCase();
		if (sentence.contains(token))
			result.set(sentence.replace(token, ""));
		return result;
	}
}