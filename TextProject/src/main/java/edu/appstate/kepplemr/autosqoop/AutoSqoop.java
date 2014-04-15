package edu.appstate.kepplemr.autosqoop;
import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;

import java.util.List;
import java.util.ResourceBundle;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MConnection;
import org.apache.sqoop.model.MConnectionForms;
import org.apache.sqoop.model.MForm;
import org.apache.sqoop.model.MInput;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MJobForms;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.sqoop.common.SqoopException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.appstate.kepplemr.main.TextUtils;

/**
 * AutoSqoop - uses Sqoop2 to import specified MySQL tables into HDFS for easy
 *             MapReduce jobs/Hive integration.
 * 
 * @author    michael
 * @version   0.0.1
 * @since     12 Feb 2014
 * 
*/
public class AutoSqoop
{
	private static final Logger log = LoggerFactory.getLogger(AutoSqoop.class);
	private SqoopClient client;
	private String hostname;
	private final String username;
	private final String password;
	private final String table;
	private final String key;
	private final String input;
	private final String output;
	private final String columns;
	private final String format;

	public static void main(String[] args) 
	{
		new AutoSqoop(args);
	}
	
	/**
	 *  Constructor -> handles CLI input, verifies proper MySQL connection to
	 *    database, calls main importation method.
	 *    
	 * @param args CLI arguments. 
	*/
	public AutoSqoop(String[] args)
	{
		DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
	    ArgumentBuilder abuilder = new ArgumentBuilder();
	    GroupBuilder gbuilder = new GroupBuilder();
	    Option mysqlName = obuilder.withLongName("mySqlName").withRequired(true).withArgument(abuilder.withName("<name>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("MySQL username to use").withShortName("n").create();
	    Option table = obuilder.withLongName("sqlTable").withRequired(true).withArgument(abuilder.withName("<table>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("SQL Table to import").withShortName("t").create();
	    Option tableKey = obuilder.withLongName("tableKey").withRequired(true).withArgument(abuilder.withName("<key>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Primary key in table").withShortName("k").create();
	    Option columns = obuilder.withLongName("columns").withRequired(true).withArgument(abuilder.withName("<col(s)>")
		    .withMinimum(1).withMaximum(1).create()).withDescription("Comma-separated list of columns to import")
		    .withShortName("c").create();
	    Option input = obuilder.withLongName("input").withRequired(false).withArgument(abuilder.withName("<dbDir>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Input directory (defaults to /var/lib/mysql)")
	    	.withShortName("i").create();
	    Option output = obuilder.withLongName("output").withRequired(false).withArgument(abuilder.withName("<hdfsDir>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Output directory (defaults to /user/$USERNAME/sqoopOut/)")
	    	.withShortName("o").create();
	    Option sqoopServer = obuilder.withLongName("sqoopServer").withRequired(false).withArgument(abuilder.withName("<server>")
	    	.withMinimum(1).withMaximum(1).create()).withDescription("Address of Sqoop RPC server").withShortName("a").create();
	    Option seqFile = obuilder.withLongName("seqFileFormat").withRequired(false)
				.withDescription("Store on HDFS in SequenceFile format").withShortName("s").create();
	    Group group = gbuilder.withName("Options").withOption(mysqlName).withOption(table).withOption(tableKey)
	    	.withOption(input).withOption(output).withOption(columns).withOption(seqFile).withOption(sqoopServer).create();
	    Parser parser = new Parser();
	    parser.setGroup(group);
	    CommandLine cmdLine = null;
	    try 
	    {
			cmdLine = parser.parse(args);
	        Process proc = Runtime.getRuntime().exec("hostname");
	        BufferedReader stdIn = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	        this.hostname = stdIn.readLine();
		} 
	    catch (OptionException ex) 
	    {
	    	log.debug("Exception on Option: ", ex);
	        TextUtils.printHelp(group);
	        System.exit(-1);
		} 
	    catch (IOException io) 
		{
			System.err.println("Could not ascertain local hostname.");
			io.printStackTrace();
			this.hostname = "localhost";
		}
	    this.username = cmdLine.getValue(mysqlName).toString();
	    this.table = cmdLine.getValue(table).toString();
	    this.key = cmdLine.getValue(tableKey).toString();
	    this.columns = cmdLine.getValue(columns).toString();
	    if (cmdLine.hasOption(input))
	    	this.input = cmdLine.getValue(input).toString();
	    else
	    	this.input = "/var/lib/mysql";
	    if (cmdLine.hasOption(output))
	    	this.output = cmdLine.getValue(output).toString();
	    else
	    	this.output = "hdfs://localhost:8020/user/" + System.getProperty("user.name") + "/sqoopOut/";
	    if (cmdLine.hasOption(seqFile))
	    	this.format = "SEQUENCE_FILE";
	    else
	    	this.format = "TEXT_FILE";
	    String address;
	    if (cmdLine.hasOption(sqoopServer))
	    	address = cmdLine.getValue(sqoopServer).toString();
	    else
	    	address = "http://localhost:12000/sqoop/";
	    this.client = new SqoopClient(address);
		Console cons = System.console();
		char[] pw = cons.readPassword("Password: ");
		this.password = new String(pw);
		if (!authenticateMysqlUser())
			System.exit(-1);
		importLocalDatabases();
	}
	
	/**
	 * authenticateMysqlUser -> Verifies that the specified username with the
	 *   specified password can in fact successfully connect to the MySQL server.
	 *   
	 * @return true if connection was successful, false otherwise. 
	*/
	private boolean authenticateMysqlUser()
	{
		try
		{
			Class.forName("com.mysql.jdbc.Driver");
			DriverManager.getConnection("jdbc:mysql://" + hostname + "/mysql", username, password);
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
	 * importLocalDatabases -> Scans the user-specified database directory 
	 *   (or /var/lib/mysql if user did not pass -i flag), initiates importation
	 *   for all databases containing matching tables.
	*/
	private void importLocalDatabases()
	{
		System.out.println("Scanning working directory : " + input);
		File currDir = new File(input);
	    for (final File fileEntry : currDir.listFiles()) 
	    {
	    	if (fileEntry.isDirectory())
	    	{
	    		File frm = new File(fileEntry.getPath() + "/" + table + ".frm");
	    		File myd = new File(fileEntry.getPath() + "/" + table + ".MYD");
	    		File myi = new File(fileEntry.getPath() + "/" + table + ".MYI");
	    		if (frm.exists() && !frm.isDirectory() && myd.exists() && 
	    			!myd.isDirectory() && myi.exists() && !myi.isDirectory())
	    		{
	    			System.out.println("Found database: " + fileEntry.getName());
	    			try
	    			{
	    				long connection = createConnection(fileEntry.getName());
	    				long jobId = createJob(connection, fileEntry.getName());
	    				submitJob(jobId);
	    			}
	    			catch (SQLException ex)
	    			{
	    				ex.printStackTrace();
	                    System.err.println("SQLState: " + ((SQLException)ex).getSQLState());
	                    System.err.println("Error Code: " + ((SQLException)ex).getErrorCode());
	                    System.err.println("Message: " + ex.getMessage());
	                    Throwable t = ex.getCause();
	                    while(t != null) 
	                    {
	                        System.out.println("Cause: " + t);
	                        t = t.getCause();
	                    }
	    			} 
	    			catch (IOException e) 
	    			{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	    		}
	    	}
	    }
	}

	/**
	 * createConnection -> Establishes JDBC connection with previously discovered
	 *   matching MySQL database. 
	 *   
	 * @param database The name of the database to connect to.
	 * @return Connection persistence ID returned from Sqoop2.
	 * @throws SQLException database cannot be found/accessed.
	*/
	private long createConnection(String database) throws SQLException, IOException
	{
		MConnection newCon = client.newConnection(1);
		MConnectionForms conForms = newCon.getConnectorPart();
		MConnectionForms frameworkForms = newCon.getFrameworkPart();
		newCon.setName("AutoSqoopConnection");
		// Prevent memory issues by telling SQL not to fetch the entire table into memory simultaneously.
		conForms.getStringInput("connection.connectionString").setValue("jdbc:mysql://" + hostname + ":3306/" + database + 
			"?&defaultFetchSize=1000&useCursorFetch=true");
		conForms.getStringInput("connection.jdbcDriver").setValue("com.mysql.jdbc.Driver");
		conForms.getStringInput("connection.username").setValue(username);
		conForms.getStringInput("connection.password").setValue(password);
		frameworkForms.getIntegerInput("security.maxConnections").setValue(0);
		Status status = client.createConnection(newCon);
		if (status.canProceed())
		{
			System.out.println("Created. New Connection ID : " + newCon.getPersistenceId());
			return newCon.getPersistenceId();
		}
		else
			throw new SQLException("Could not connect to database", "");
	}

	/**
	 * createJob -> configured the Sqoop job to be submitted, specifies tables,
	 *   columns to import and desired output (SEQUENCE_FILE, TEXT_FILE, etc).
	 *   
	 * @param connectionId -> Connection persistence ID.
	 * @param db -> Name of the MySQL database to import from.
	 * @return -> Job persistence ID returned from Sqoop2.
	*/
	private long createJob(long connectionId, String db)
	{
		MJob newjob = client.newJob(connectionId, org.apache.sqoop.model.MJob.Type.IMPORT);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();
		newjob.setName("ImportJob");
		connectorForm.getStringInput("table.schemaName").setValue("");
		connectorForm.getStringInput("table.tableName").setValue(table);
		connectorForm.getStringInput("table.columns").setValue(columns);
		connectorForm.getStringInput("table.partitionColumn").setValue(key);
		frameworkForm.getEnumInput("output.storageType").setValue("HDFS");
		frameworkForm.getEnumInput("output.outputFormat").setValue(format);
		frameworkForm.getStringInput("output.outputDirectory").setValue(output + db);
		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);
		Status status = client.createJob(newjob);
		describe(client.getConnection(connectionId).getConnectorPart().getForms(), client.getResourceBundle(1));
		describe(client.getConnection(connectionId).getFrameworkPart().getForms(), client.getFrameworkResourceBundle());
		printMessage(newjob.getConnectorPart().getForms());
		printMessage(newjob.getFrameworkPart().getForms());
		if (status.canProceed())
		{
			System.out.println("New Job ID: " + newjob.getPersistenceId());
			return newjob.getPersistenceId();
		}
		else
			throw new SqoopException(null, "Coud not create Sqoop job.");
	}

	/**
	 *  submitJob -> submits the configured Sqoop2 job to the Sqoop-server. 
	 *    Displays Hadoop information / Job ID.
	 *    
	 * @param jobId Job persistence ID
	*/
	private void submitJob(long jobId) 
	{
		System.out.println("Submitting job to server...");
		MSubmission submission = client.startSubmission(jobId);
		System.out.println("Status : " + submission.getStatus());
		if (submission.getStatus().isRunning() && submission.getProgress() != -1)
			System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		System.out.println("Hadoop job id :" + submission.getExternalId());
		System.out.println("Job link : " + submission.getExternalLink() + "\n");
		Counters counters = submission.getCounters();
		if (counters != null) 
		{
			System.out.println("Counters:");
			for (CounterGroup group : counters) 
			{
				System.out.print("\t");
				System.out.println(group.getName());
				for (Counter counter : group) 
				{
					System.out.print("\t\t");
					System.out.print(counter.getName());
					System.out.print(": ");
					System.out.println(counter.getValue());
				}
			}
		}
		if (submission.getExceptionInfo() != null)
		{
			System.out.println("Exception info : " + submission.getExceptionInfo());
			System.out.println(submission.getExceptionStackTrace());
		}
		
	}

	/**
	 *  printMessage -> displays error or warning messages from connector part
	 *    forms and framework part forms. 
	 *    
	 * @param formList List of forms from framework and connector. Scans these
	 *   for errors or warnings.
	*/
	private static void printMessage(List<MForm> formList) 
	{
		for (MForm form : formList) 
		{
			List<MInput<?>> inputlist = form.getInputs();
			if (form.getValidationMessage() != null) 
				System.out.println("Form message: " + form.getValidationMessage());
			for (MInput<?> minput : inputlist) 
			{
				if (minput.getValidationStatus() == Status.ACCEPTABLE) 
					System.out.println("Warning:" + minput.getValidationMessage());
				else if (minput.getValidationStatus() == Status.UNACCEPTABLE) 
					System.out.println("Error:" + minput.getValidationMessage());
			}
		}
	}
	
	/**
	 *  describe -> displays Connection configuration information
	 *  @param forms
	 *  @param resource
	*/
	void describe(List<MForm> forms, ResourceBundle resource) 
	{
		  for (MForm mf : forms) 
		  {
			  System.out.println(resource.getString(mf.getLabelKey())+":");
			  List<MInput<?>> mis = mf.getInputs();
			  for (MInput<?> mi : mis)
				  System.out.println(resource.getString(mi.getLabelKey()) + " : " + mi.getValue());
		  }
	}
}
