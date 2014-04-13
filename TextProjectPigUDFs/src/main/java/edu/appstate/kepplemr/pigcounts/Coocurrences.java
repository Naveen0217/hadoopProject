package edu.appstate.kepplemr.pigcounts;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Coocurrences extends EvalFunc<String> 
{
	@Override
	public String exec(Tuple tuple) throws IOException 
	{
		if (tuple == null || tuple.size() == 0)
			return null;
		String line = ((String)tuple.get(0)).toLowerCase();
		String word = (String) tuple.get(1);
		// STDOUT, STDERR -> HDFS logDir
		if (line.contains(word))
			return line;
		return null;
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException 
	{
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		Schema scheme = new Schema();
		scheme.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		scheme.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		funcList.add(new FuncSpec(this.getClass().getName(), scheme));
		return funcList;
	}
}