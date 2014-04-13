package edu.appstate.kepplemr.hivecounts;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class RightNeighbors extends UDF
{
	private Text result = new Text();
	
	public Text evaluate(Text line, Text word)
	{
		String sentence = line.toString().toLowerCase();
		String token = word.toString();
		if (sentence.contains(token))
		{
			String[] words = sentence.split(" ");
			for (int i = 0; i < words.length; i++)
				if (words[i].equals(token) && (i+1 < words.length))
				{
					result.set(words[i+1]);
					return result;
				}
		}
		return null;
	}

}