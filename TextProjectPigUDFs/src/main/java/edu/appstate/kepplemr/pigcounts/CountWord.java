package edu.appstate.kepplemr.pigcounts;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class CountWord extends LoadFunc 
{
	private String word;
	private final TupleFactory tupleFactory = TupleFactory.getInstance();
	@SuppressWarnings("rawtypes")
	private RecordReader reader;

	public CountWord(String word) 
	{
		this.word = word.toLowerCase();
	}

	@Override
	public void setLocation(String location, Job job) throws IOException 
	{
		FileInputFormat.setInputPaths(job, location);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public InputFormat getInputFormat() 
	{
		return new TextInputFormat();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) 
	{
		this.reader = reader;
	}

	@Override
	public Tuple getNext() throws IOException 
	{
		try 
		{
			if (!reader.nextKeyValue())
				return null;
			String line = ((Text) reader.getCurrentValue()).toString().toLowerCase();
			Tuple tuple = tupleFactory.newTuple(1);
			int index = line.indexOf(word);
			int count = 0;
			while (index != -1) 
			{
			    count++;
			    line = line.substring(index + 1);
			    index = line.indexOf(word);
			}
			tuple.set(0, count);
			return tuple;
		} 
		catch (InterruptedException e) 
		{
			throw new ExecException(e);
		}
	}

}
