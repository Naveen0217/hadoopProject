package edu.appstate.kepplemr.customanalyzer;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

// Note: Must compile against Lucene 4.6.1 to work with Mahout.
public class GutenbergAnalyzer extends StopwordAnalyzerBase
{
	public GutenbergAnalyzer()
	{
		super(Version.LUCENE_46, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
	}
    
    public GutenbergAnalyzer(CharArraySet stopSet)
    {
    	super(Version.LUCENE_46, stopSet);
    }
    
	public static void main(String[] args) throws Exception 
    {
        GutenbergAnalyzer analyzer = new GutenbergAnalyzer();
        String str = "I am happy to join with you today running ran runs 3.14 453 0.45 .4556";
        Reader reader = new StringReader(str);
        TokenStream stream = analyzer.tokenStream("", reader);
        while (stream.incrementToken()) 
        {
            CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
            System.out.print(term.toString() + "\t");
        }
        System.out.println();
        analyzer.close();
    }
    
    static class ToughFilter extends FilteringTokenFilter
    {
    	private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);

		public ToughFilter(Version version, TokenStream input) 
		{
			super(version, input);
		}
		
		@Override
		public boolean accept() throws IOException 
		{
			// Remove numbers and all tokens containing non-alpha characters.
			return StringUtils.isAlpha(termAtt.toString());
			//return !termAtt.toString().matches("-?\\d+(\\.\\d+)?");
		}
    }

	@Override
	protected TokenStreamComponents createComponents(String arg0, Reader reader) 
	{
		TokenStream result = new StandardTokenizer(Version.LUCENE_46, reader);
		result = new StandardFilter(Version.LUCENE_46, result);
        result = new ToughFilter(Version.LUCENE_46, result);
        result = new LowerCaseFilter(Version.LUCENE_46, result);
        result = new LengthFilter(Version.LUCENE_46, result, 3, 20);
        result = new StopFilter(Version.LUCENE_46, result, StandardAnalyzer.STOP_WORDS_SET);
        result = new PorterStemFilter(result);
		return null;
	}
}
