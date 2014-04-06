package edu.appstate.kepplemr.main;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.util.HelpFormatter;

/**
 * TextUtils -> contains common static functions available to all classes in the
 *   project. 
 * 
 * @author  michael
 * @version 1.1
 * @since   04 Apr 2014
*/
public class TextUtils
{
	public static void printHelp(Group group) 
	{
		HelpFormatter formatter = new HelpFormatter();
		formatter.setGroup(group);
		formatter.print();
	}
}
