package edu.appstate.kepplemr.autosqoop;
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
import java.io.Console;
import java.io.File;

import org.apache.sqoop.common.SqoopException;

/**
 * AutoSqoop - Placed in parent directory, will import all child SQL database
 * folders through Sqoop into HDFS/Hive.
 * 
 * @author    michael
 * @version   0.0.1
 * @since     12 Feb 2014
 * @see       http://sqoop.apache.org/docs/1.99.2/ClientAPI.html 
 * 
 * 
*/
public class AutoSqoop 
{
	String url = "http://mothership:12000/sqoop/";
	SqoopClient client = new SqoopClient(url);
	private final String username;
	private final String password;
	private final String table;
	private final String key;

	public static void main(String[] args) 
	{
		if (args.length != 3)
		{
			System.err.println("Usage: java -jar AutoSqoop.jar <MySQL user> <tableToImport> <key column>");
			System.exit(-3);
		}
		new AutoSqoop(args);
	}
	
	public AutoSqoop(String[] args)
	{
		this.username = args[0];
		this.table = args[1];
		this.key = args[2];
		Console cons = System.console();
		char[] pw = cons.readPassword("Password: ");
		this.password = new String(pw);
		if (!authenticateMysqlUser())
			System.exit(-1);
		importLocalDatabases();
	}
	
	// Check that username and password can connect to local MySQL server.
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
	
	// Scan working directory for databases containing the tables we want to import
	private void importLocalDatabases()
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
	    			!myd.isDirectory() && myi.exists() && !myi.isDirectory())
	    		{
	    			System.out.println("Found database: " + fileEntry.getName());
	    			try
	    			{
	    				long connection = createConnection(fileEntry.getName());
	    				long jobId = createJob(connection, fileEntry.getName());
	    				submitJob(jobId);
	    			}
	    			catch (Exception ex)
	    			{
	    				ex.printStackTrace();
	    			}
	    		}
	    	}
	    }
	}

	// Establish connection with specific MySQL database containing target table
	private long createConnection(String database) throws SQLException
	{
		MConnection newCon = client.newConnection(1);
		MConnectionForms conForms = newCon.getConnectorPart();
		MConnectionForms frameworkForms = newCon.getFrameworkPart();
		newCon.setName("AutoSqoopConnection");
		conForms.getStringInput("connection.connectionString").setValue("jdbc:mysql://localhost/" + database);
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

	// Configure the Sqoop Job to be submitted.
	private long createJob(long connectionId, String db)
	{
		MJob newjob = client.newJob(connectionId, org.apache.sqoop.model.MJob.Type.IMPORT);
		MJobForms connectorForm = newjob.getConnectorPart();
		MJobForms frameworkForm = newjob.getFrameworkPart();
		newjob.setName("ImportJob");
		connectorForm.getStringInput("table.schemaName").setValue("");
		connectorForm.getStringInput("table.tableName").setValue(table);
		//
		connectorForm.getStringInput("table.columns").setValue("s_id,sentence");
		connectorForm.getStringInput("table.partitionColumn").setValue(key);
		frameworkForm.getEnumInput("output.storageType").setValue("HDFS");
		//frameworkForm.getEnumInput("output.outputFormat").setValue("TEXT_FILE");
		frameworkForm.getEnumInput("output.outputFormat").setValue("SEQUENCE_FILE");
		// Set to HCatalog Metastore Warehouse dir
		frameworkForm.getStringInput("output.outputDirectory").setValue("hdfs://mothership:8020" + "/user/root/hive/" + db);
		frameworkForm.getIntegerInput("throttling.extractors").setValue(1);
		frameworkForm.getIntegerInput("throttling.loaders").setValue(1);
		Status status = client.createJob(newjob);
		
		//////////////////
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

	// Submit the Sqoop job.
	private void submitJob(long jobId) 
	{
		MSubmission submission = client.startSubmission(jobId);
		System.out.println("Status : " + submission.getStatus());
		if (submission.getStatus().isRunning() && submission.getProgress() != -1)
			System.out.println("Progress : " + String.format("%.2f %%", submission.getProgress() * 100));
		System.out.println("Hadoop job id :" + submission.getExternalId());
		System.out.println("Job link : " + submission.getExternalLink());
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
	
	void describe(List<MForm> forms, ResourceBundle resource) 
	{
		  for (MForm mf : forms) {
		    System.out.println(resource.getString(mf.getLabelKey())+":");
		    List<MInput<?>> mis = mf.getInputs();
		    for (MInput<?> mi : mis) {
		      System.out.println(resource.getString(mi.getLabelKey()) + " : " + mi.getValue());
		    }
		    System.out.println();
		  }
		}
}
