package edu.appstate.kepplemr.dataimport;
import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.Statement;

import edu.appstate.kepplemr.main.TextUtils;

/**
 * SQL2Seq -> Converts SQL import data to <Text,Text>-formatted SequenceFiles
 *   appropriate for use with Mahout.
 * 
 * @author    michael
 * @version   0.0.1
 * @since     12 Feb 2014
*/
public class SQL2Seq 
{
	private static final Logger log = LoggerFactory.getLogger(SQL2Seq.class);
	private final String input;
	private final String hdfsOutdir;
	private final String username;
	private final String password;
	private final String columns;
	private final String table;
	private final boolean intKey;
	private int numProcessed;
	
	public static void main(String[] args)
	{
		new SQL2Seq(args);
	}
	
	public SQL2Seq(String[] args)
	{
		// 
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    Option mysqlName = obuilder.withLongName("mySqlName").withRequired(true).withArgument(abuilder.withName("mysqlName")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("MySQL username to use").withShortName("n").create();
	    Option table = obuilder.withLongName("sqlTable").withRequired(true).withArgument(abuilder.withName("sqlTable")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("SQL Table to import").withShortName("t").create();
	    Option columns = obuilder.withLongName("columns").withRequired(true).withArgument(abuilder.withName("columns")
		    .withMinimum(1).withMaximum(1).create()).withDescription("Comma-separated list of columns to import")
		    .withShortName("c").create();
	    Option input = obuilder.withLongName("input").withRequired(false).withArgument(abuilder.withName("input")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Input directory (defaults to /var/lib/mysql)")
	    	.withShortName("i").create();
	    Option output = obuilder.withLongName("output").withRequired(false).withArgument(abuilder.withName("output")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Output directory (defaults to /user/$USERNAME/sqoopOut/)")
	    	.withShortName("o").create();
	    Option intWritable = obuilder.withLongName("intWritable").withRequired(false)
			.withDescription("Generate IntWritable keys").withShortName("i").create();
	    Group group = gbuilder.withName("Options").withOption(mysqlName).withOption(table)
	    	.withOption(input).withOption(output).withOption(columns).create();
	    Parser parser = new Parser();
	    parser.setGroup(group);
	    CommandLine cmdLine = null;
	    try 
	    {
			cmdLine = parser.parse(args);
		} 
	    catch (OptionException ex) 
	    {
	    	log.debug("Exception: ", ex);
	        TextUtils.printHelp(group);
	        System.exit(-1);
		}
	    this.username = cmdLine.getValue(mysqlName).toString();
	    this.table = cmdLine.getValue(table).toString();
	    this.columns = cmdLine.getValue(columns).toString();
	    this.intKey = cmdLine.hasOption(intWritable);
	    if (cmdLine.hasOption(input))
	    	this.input = cmdLine.getValue(input).toString();
	    else
	    	this.input = "/var/lib/mysql";
	    if (cmdLine.hasOption(output))
	    	this.hdfsOutdir = cmdLine.getValue(output).toString();
	    else
	    	this.hdfsOutdir = "hdfs://localhost:8020/user/" + System.getProperty("user.name") + "/sql2seqOut/";
		Console cons = System.console();
		char[] pw = cons.readPassword("Password: ");
		this.password = new String(pw);
		if (!authenticateMysqlUser())
			System.exit(-1);
		processDatabases();
	}
	
	/** 
	 *  authenticateMysqlUser - ensures that username & password combo can 
	 *   communicate with local MySQL daemon.
	 *     
	 *  @return - true if successful, false otherwise.
	*/
	private boolean authenticateMysqlUser()
	{
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
			DriverManager.getConnection("jdbc:mysql://localhost/mysql", username, password);
		}
		catch (SQLException ex)
		{
			System.err.println("Error: could not connect to MySQL database with provided credentials.");
			ex.printStackTrace();
			return false;
		}
		catch (ClassNotFoundException ex)
		{
			System.err.println("Error: could not load MySQL driver.");
			ex.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 *  processDatabases - main execution loop for searching for matching SQL
	 *    databases, processing them, and calling methods to create the 
	 *    SequenceFiles in HDFS.
	*/
	private void processDatabases()
	{
		System.out.println("Scanning working directory : " + input);
		File currDir = new File(input);
	    for (final File fileEntry : currDir.listFiles()) 
	    {
	    	try 
	    	{
				if (fileEntry.getCanonicalFile().isDirectory())
				{
					File frm = new File(fileEntry.getPath() + "/" + table + ".frm");
					File myd = new File(fileEntry.getPath() + "/" + table + ".MYD");
					File myi = new File(fileEntry.getPath() + "/" + table + ".MYI");
					if (frm.exists() && !frm.isDirectory() && myd.exists() && 
						!myd.isDirectory() && myi.exists() && !myi.isDirectory() &&
						(fileEntry.getName().contains("_20") || fileEntry.getName().contains("_19")))
					{
						System.out.println("Found matching database: " + fileEntry.getName());
						String dbName = fileEntry.getName();
						if (dbName.contains("-mysql"))
							dbName = renameDatabase(fileEntry);
						// sanitizeDb()?
						dumpFieldsToFile(dbName);
						createSeqFiles(dbName);
					}
				}
			} 
	    	catch (IOException e) 
	    	{
	    		System.err.println("I/O Error on file/dir: " + fileEntry.toString());
				e.printStackTrace();
			}
	    }
	}
	
	/**
	 *  createSeqFiles - for a database, processes its dump file and loads values
	 *    into SequenceFile on HDFS. Text key value is the name of the database. 
	 *    
	 * @param database Name of the database we'll load into HDFS.
	*/
	private void createSeqFiles(String database)
	{
		SequenceFile.Writer writer = null;
		Configuration conf = new Configuration();
		// When packaging with maven-assembly, included JARs can overwrite the proper conf settings; reset them. 
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		Path path = new Path(hdfsOutdir + numProcessed);
		IntWritable iKey = null;
		Text tKey = null;
		try 
		{
			if (intKey)
			{
				System.out.println("Creating <IntWritable, Text> Sequencefiles...");
				writer = SequenceFile.createWriter(conf, Writer.file(path), Writer.keyClass(IntWritable.class), Writer.valueClass(Text.class));
				iKey = new IntWritable();
			}
			else
			{
				System.out.println("Creating <Text, Text> Sequencefiles...");
				writer = SequenceFile.createWriter(conf, Writer.file(path), Writer.keyClass(Text.class), Writer.valueClass(Text.class));
				tKey = new Text();
			}
			Text value = new Text();
			String sep = System.getProperty("file.separator");
			String dumpFile = input + sep + database + sep + "dump.txt";
			File file = new File(dumpFile);
			Scanner scan = new Scanner(file);
			while (scan.hasNextLine())
			{
				if (intKey)
				{
					iKey.set(numProcessed);
					value.set(scan.nextLine());
					writer.append(iKey, value);
				}
				else
				{
					tKey.set(database + numProcessed);
					value.set(scan.nextLine());
					writer.append(tKey, value);
				}
				numProcessed++;
			}
			scan.close();
		} 
		catch (FileNotFoundException ex) 
		{
			System.err.println("Database dump file not found.");
			ex.printStackTrace();
		} 
		catch (IOException ex) 
		{
			System.err.println("Error writing to filesystem.");
			ex.printStackTrace();
		}
		finally
		{
			IOUtils.closeStream(writer);
		}
	}
	
	/**
	 * dumpFieldsToFile -> for particular database, dumps the specified fields to
	 *   a file.
	 *   
	 * @param database Name of the database we're to process.
	*/
	private void dumpFieldsToFile(String database)
	{
		java.sql.Connection conn = null;
		try
		{
			String sep = System.getProperty("file.separator");
			String dumpFile = input + sep + database + sep + "dump.txt";
			File rmFile = new File(dumpFile);
			if (rmFile.delete())
				System.out.println("Previous dump file removed -> " + dumpFile);
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost/" +database, username, password);
		    Statement st = (Statement) conn.createStatement();
		    st.executeQuery("SELECT " + columns + " FROM " + table + " INTO OUTFILE 'dump.txt' LINES TERMINATED BY '\r\n';");
		    System.out.println("Created out file for database -> " + database);
		}
		catch (Exception ex)
		{
			System.out.println("Could not create out file for database -> " + database);
			ex.printStackTrace();
		}  
		finally 
		{
			try 
			{
				conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}
		
	/**
	 * renameDatabase -> convenience method to rename databases by stripping
	 *   the annoying '-mysql' suffix. 
	 *     
	 * Note:This also avoids problems with new MySQL 5.1 encodings, which would
	 *   otherwise require ALTER DATABASE UPGRADE DIRECTORY NAME to re-encode
	 *   the '-' character found in the Corpora databases as Unicode '@002d',
	 *   making it safe for all systems. 
	 *     
	 * @param database the original file before renaming.
	 * @return The name of the database, whether it was modified or not.
	 * @throws IOException
	*/
	private String renameDatabase(File database) throws IOException
	{
		String oldName = database.getCanonicalPath();
		String newName = oldName.substring(0, oldName.length() - 6);
		newName = newName.replace('-','_');
		File newFile = new File(newName);
		if (database.renameTo(newFile))
			return newFile.getName();
		else
		{
			System.err.println("Error renaming database -> " + oldName);
			System.err.println("Check privileges on database");
			return database.getName();
		}
	}
}
