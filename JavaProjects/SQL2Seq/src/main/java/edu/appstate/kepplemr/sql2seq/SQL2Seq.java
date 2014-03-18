package edu.appstate.kepplemr.sql2seq;
import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import com.mysql.jdbc.Statement;

public class SQL2Seq 
{
	private final String hdfsOutdir;
	private final String username;
	private final String password;
	private final String fields;
	private final String table;
	//private int numProcessed;
	
	public static void main(String[] args)
	{
		new SQL2Seq();
	}
	
	public SQL2Seq()
	{
		System.out.println("*** Welcome to SQL2Seq ***");
		Console console = System.console();
		this.username = console.readLine("MySQL username: ");
		char[] pw = console.readPassword("Password: ");
		this.password = new String(pw);
		this.table = console.readLine("Table to import from: ");
		this.fields = console.readLine("Field(s) to import: ");
		this.hdfsOutdir = console.readLine("HDFS Output directory: ");
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
		String workingDir = System.getProperty("user.dir");
		System.out.println("Scanning working directory : " + workingDir);
		File currDir = new File(workingDir);
	    for (final File fileEntry : currDir.listFiles()) 
	    {
	    	if (fileEntry.isDirectory())
	    	{
	    		File frm = new File(fileEntry.getPath() + "/" + table + ".frm");
	    		File myd = new File(fileEntry.getPath() + "/" + table + ".MYD");
	    		File myi = new File(fileEntry.getPath() + "/" + table + ".MYI");
	    		if (frm.exists() && !frm.isDirectory() && myd.exists() && 
	    			!myd.isDirectory() && myi.exists() && !myi.isDirectory() &&
	    			fileEntry.getName().contains("_200"))
	    		{
	    			System.out.println("Found matching database: " + fileEntry.getName());
	    			try
	    			{
	    				String dbName = fileEntry.getName();
	    				if (dbName.contains("-mysql"))
	    					dbName = renameDatabase(fileEntry);
	    				// sanitizeDb()?
	    				dumpFieldsToFile(dbName);
	    				createSeqFiles(dbName);
	    			}
	    			catch (Exception ex)
	    			{
	    				ex.printStackTrace();
	    			}
	    		}
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
	    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
		Path path = new Path(hdfsOutdir);
		Text key = new Text(database);
		Text value = new Text();
		String sep = System.getProperty("file.separator");
		String dumpFile = System.getProperty("user.dir") + sep + database + sep + "dump.txt";
		File file = new File(dumpFile);
		try 
		{
			Scanner scan = new Scanner(file);
			writer = SequenceFile.createWriter(conf, Writer.file(path), Writer.keyClass(Text.class), Writer.valueClass(Text.class));
			key.set(database);
			while (scan.hasNextLine())
			{
				value.set(scan.nextLine());
				writer.append(key, value);
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
			String dumpFile = System.getProperty("user.dir") + sep + database + sep + "dump.txt";
			File rmFile = new File(dumpFile);
			if (rmFile.delete())
				System.out.println("Previous dump file removed -> " + dumpFile);
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://localhost/" +database, username, password);
		    Statement st = (Statement) conn.createStatement();
		    st.executeQuery("SELECT " + fields + " FROM " + table + " INTO OUTFILE 'dump.txt' LINES TERMINATED BY '\r\n';");
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
	*/
	private String renameDatabase(File database)
	{
		String oldName = database.getName();
		String newName = oldName.substring(0, oldName.length() - 6);
		File newFile = new File(newName);
		if (database.renameTo(newFile))
			return newName;
		else
		{
			System.err.println("Error renaming database -> " + oldName);
			System.err.println("Check privileges on database");
			return oldName;
		}
	}
}
