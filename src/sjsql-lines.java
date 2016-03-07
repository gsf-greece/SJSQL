/*
 * @(#)sjsql.java (Metawerx tools)
 *
 * Copyright (c) 2006 Metawerx Pty Ltd. All Rights Reserved.
 *
 * This software is the confidential and proprietary information of Metawerx
 * ("Confidential Information").  You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with Metawerx.
 *
 * METAWERX MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY
 * OF THE SOFTWARE OR THE SOURCE CODE, EITHER EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. METAWERX SHALL NOT BE LIABLE
 * FOR ANY DAMAGES SUFFERED BY ANY PARTY AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE AND DOCUMENTATION OR ITS DERIVATIVES
 *
 * Usage (Windows) (note java classpath uses semicolon (;) to separate paths)
 *     java -cp .;jdbcDriver.jar sjsql driverClass jdbcString userName passWord inputFile
 *
 * Usage (Linux) (note java classpath uses colon (:) to separate paths)
 *     java -cp .:jdbcDriver.jar sjsql driverClass jdbcString userName passWord inputFile
 *
 * Example for SQL/Server with jTDS
 *  java -cp .;jtds-1.2.jar sjsql net.sourceforge.jtds.jdbc.Driver jdbc:jtds:sqlserver://localhost:1433 user pass infile.sql
 *
 * Example for MySQL with mysql-connector
 *  java -cp .;mysql-connector-java-3.1.13-bin.jar sjsql com.mysql.jdbc.Driver jdbc:mysql://localhost:3306/myDatabase user pass infile.sql
 *
 * Description:
 *    Connect to database
 *    Run each line from input file as a query
 *  Log output from each query to output file
 *
 */

import java.io.*;
import java.sql.*;

public class sjsql {

    // Required Parameters
    public String driverClassName = null;
    public String connectStr = null;
    public String dsuser = null;
    public String dspass = null;
    public String fileName = null;

    // Switches
    public int startLine = 1;
    public int batchSize = 0;
    public boolean ignoreNoData = false;
    public boolean logQueries = false;
    public boolean useTransaction = false;

    // Internal
    public boolean initialisedOk = false;
    public int batchCount = 0;
    public int batchStart = 0;
    public int lastCommitted = 0;
    public String driverError;
    public Connection c = null;
    public Statement s = null;

    // Variables used by processing routing
    boolean error = false;
    int lineNumber = 1;
    int numberOfLines = 0;
    String sqlQuery;
    StringBuffer sqlBatch = new StringBuffer();
    ResultSet resultSet;
    ResultSetMetaData resultMeta;

    // Wrapper for starting from commmand line
    public static void main(String s[]) {
        sjsql obj = new sjsql();
        obj.nonStaticMain(s);
    }

    // Non-static method, allowing re-use from within a threaded application
    public void nonStaticMain(String s[]) {

        // Parse command line settings
        if(s.length < 5) {
            System.err.println("sjsql - Simple JDBC command line SQL batch tool (multi-database)");
            System.err.println("Created by Neale Rudd, Metawerx Pty Ltd, 2006");
            System.err.println("This is free software, with absolutely no warranty or support");
            System.err.println("");
            System.err.println("Usage: java -cp <classPath> sjsql <driverClass> <jdbcString> <userName> <password> <sqlFile>");
            System.err.println("  driverClass - JDBC driver class name, eg: com.mysql.jdbc.Driver");
            System.err.println("  jdbcString - JDBC connection string, eg: jdbc:mysql://localhost:3306/myDatabase");
            System.err.println("  userName - database username");
            System.err.println("  password - database password");
            System.err.println("  sqlFile - list of SQL statements, one per line");
            System.err.println("Switches (add at end of line):");
            System.err.println("  --start 123 - line number to start from, for resuming after errors. Default is 1.");
            System.err.println("  --lines 45 - number of lines to be parsed, for resuming after errors. Default is all lines.");
            System.err.println("  --ignore-nodata - if query response is no-data (eg: INSERT statements) then don't log. Default is log all results.");
            System.err.println("  --log-queries - include each query in the output. Default is to only log results.");
            System.err.println("  --batch 500 - send 500 queries at a time, wrapped in a transaction (good for remote connections).");
            System.err.println("  --batch all - wrap entire file in a transaction, call rollback on failure.");
            System.err.println("Notes:");
            System.err.println("  Line numbers start from 1 (not 0)");
            System.err.println("  Each line is treated as a separate query.  Semicolons or other query-terminators are optional and will be ignored.");
            System.err.println("  Query logging is sent to System.out (can be redirected with >results.log)");
            System.err.println("  Other output is sent to System.err (can be redirected with 2>output.log)");
            System.exit(0);
        }
        driverClassName = s[0];
        connectStr = s[1];
        dsuser = s[2];
        dspass = s[3];
        fileName = s[4];
        if(s.length > 5) {
            // Switches provided
            for(int loop = 5; loop < s.length; loop++) {
                if(s[loop].equals("--start")) {
                    try {
                        startLine = Integer.parseInt(s[loop+1]);
                    } catch(Throwable e) {
                        System.err.println("[sjsql] ERROR: start line must be numeric: "+s[loop+1]);
                        System.exit(1);
                    }
                    // Skip next arg
                    loop++;
                } else if(s[loop].equals("--lines")) {
                    try {
                        numberOfLines = Integer.parseInt(s[loop+1]);
                        //System.out.println("[sjsql] ERROR: lines: "+numberOfLines);
                    } catch(Throwable e) {
                        System.err.println("[sjsql] ERROR: number of lines must be numeric: "+s[loop+1]);
                        System.exit(1);
                    }
                    // Skip next arg
                    loop++;
                } else if(s[loop].equals("--batch")) {
                    try {
                        if(s[loop+1].equals("all"))
                            useTransaction = true;
                        else
                            batchSize = Integer.parseInt(s[loop+1]);
                    } catch(Throwable e) {
                        System.err.println("[sjsql] ERROR: batch size must be all or numeric: "+s[loop+1]);
                        System.exit(1);
                    }
                    // Skip next arg
                    loop++;
                } else if(s[loop].equals("--ignore-nodata")) {
                    // Ignore logging for no-data responses
                    ignoreNoData = true;
                } else if(s[loop].equals("--log-queries")) {
                    // Log all queries to standard out
                    logQueries = true;
                } else {
                    // Unknown command line switch
                    System.err.println("[sjsql] ERROR: unknown command line switch: "+s[loop]);
                    System.exit(1);
                }
            }
        }

        // Standardise file paths
        fileName = fileName.replace('\\', '/');

        // Check file exists
        if(!new File(fileName).exists()) {
            System.err.println("[sjsql] ERROR: file does not exist");
            System.exit(1);
        }

        // Initialise DB
        initDB();

        // Exit if things went wrong
        if(!initialisedOk) {
            System.err.println("[sjsql] Could not initialise DB - exiting: "+driverError);
            System.exit(1);
        }

        // Open the file
        try {
            BufferedReader infile = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));

            try {
                // Connect
                connect();
                System.err.println("[sjsql] Connected, file: "+fileName+", start: "+startLine+", lines: "+numberOfLines);

                // Wrap in transaction if requested
                if(useTransaction) {
                    startTransaction();
                }

                // Execute lines
                while((sqlQuery = infile.readLine()) != null) {
                    if (numberOfLines > 0 && (startLine + numberOfLines) <= lineNumber) {
                        break;
                    }
                    if(startLine <= lineNumber) {

                        if(batchSize == 0){
                            if(logQueries){
                                System.out.println("["+lineNumber+"] "+sqlQuery);
                            }
                        }

                        sqlQuery = sqlQuery.trim();
                        if (sqlQuery.length() == 0){
                            lineNumber++;
                            continue;
                        }

                        if(batchSize > 0) {

                            // Drop a line first in case the previous line is "GO", ends with a comment, or similar
                            // Add semicolon to terminate statement
                            sqlQuery += "\n;\n";
/*                            if(!sqlQuery.endsWith(";")) {
                            }
*/

                            // Add to batch
                            sqlBatch.append(sqlQuery);
                            batchCount++;

                            // If first line of batch, remember start line
                            if(batchCount == 1)
                                batchStart = lineNumber;

                            // If batch size reached, run batch
                            if(batchCount == batchSize) {

                                // Get batch from buffer
                                sqlQuery = sqlBatch.toString();
                                if(logQueries)
                                    System.out.println("["+batchStart+"-"+lineNumber+"] "+sqlQuery);

                                // Run queries, log results, commit
                                startTransaction();
                                process();
                                commit();
                                lastCommitted = lineNumber;

                                System.err.println("Ran batch "+batchStart+"-"+lineNumber);

                                // Reset for next batch
                                batchCount = 0;
                                sqlBatch.setLength(0);
                            }

                        } else {

                            // Single line mode
                            process();
                            lastCommitted = lineNumber;
                        }
                    }
                    lineNumber++;
                }

                // Final batch
                if(batchCount > 0) {

                    // Get batch from buffer
                    sqlQuery = sqlBatch.toString();
                    if(logQueries)
                        System.out.println("["+batchStart+"-"+lineNumber+"] "+sqlQuery);

                    // Line number is 1 higher than it should be now, so reduce by one before logging results
                    lineNumber--;

                    // Run queries, log results, commit
                    startTransaction();
                    process();
                    commit();
                    lastCommitted = lineNumber;
                }

                if(useTransaction) {
                    // Commit transaction
                    commit();
                    lastCommitted = lineNumber;
                }

            } catch(Throwable e) {
                error = true;
                System.err.println("[sjsql] Exception at line "+lineNumber+": "+e.toString());
                e.printStackTrace();
            } finally {
                try { rollback(); } catch(Exception e) {} catch(Error e) {}
                try { cleanup(); } catch(Exception e) {} catch(Error e) {}
                //System.err.println("[sjsql] Connection closed");
            }
            if(!error) {
                 if(useTransaction)
                     System.err.println("[sjsql] Committed");
                 System.err.println("[sjsql] Done");
            } else {
                 if(useTransaction || batchCount > 0)
                     System.err.println("[sjsql] Errors occurred - changes rolled back");
                 System.err.println("[sjsql] Errors occurred - exiting, start next run from line "+(lastCommitted+1)+" with the argument --start "+(lastCommitted+1));
             }

         } catch(Throwable e) {
             System.err.println("[sjsql] ERROR: Could not open file: "+fileName);
             error = true;
         }
         System.exit(error?1:0);
    }

    public void process() throws SQLException {

        resultSet = execute(sqlQuery);

        if(resultSet != null) {
            while(resultSet != null) {
                resultMeta = resultSet.getMetaData();
                int count = resultMeta.getColumnCount();
                int rows = 0;
                System.out.println("["+lineNumber+"] -- results follow --");
                if(count > 0) {
                    for(int j = 1; j <= count; j++) {
                        if(j > 1)
                            System.out.print("\t");
                        System.out.print(resultMeta.getColumnName(j));
                    }
                    System.out.println("");
                    while (resultSet.next()) {
                        for(int i = 1; i <= count; i++) {
                            if(i > 1)
                                System.out.print("\t");
                            System.out.print(resultSet.getString(resultMeta.getColumnName(i)));
                        }
                        System.out.println("");
                        rows++;
                    }
                } else
                    System.out.println("["+lineNumber+"] -- query returned empty recordset --");

                if(rows == 0)
                    System.out.println("["+lineNumber+"] -- query returned no rows --");

                // Get more results
                resultSet = getNextResultSet();
            }

        } else {
            if(!ignoreNoData)
                System.out.println("["+lineNumber+"] -- query returned no data --");
        }
    }

    public void connect() throws SQLException {

        // Load class and connect
        try {
            c = DriverManager.getConnection(connectStr, dsuser, dspass);
        } catch (SQLException se) {
            throw new SQLException("[sjsql] Exception connecting to datasource URL: " + se.toString());
        } catch (Error ce) {
            throw new SQLException("[sjsql] Error connecting to datasource URL: " + ce.toString());
        } catch (Exception ce) {
            throw new SQLException("[sjsql] Exception connecting to datasource URL: " + ce.toString());
        }

        // Create a statement
        if(c != null) {
            //Select a database
            //c.setCatalog(userdb);
            s = c.createStatement();
        }
    }

    private void initDB() {

        if(!initialisedOk) {
            try {
                Class.forName(driverClassName).newInstance();
                initialisedOk = true;
            } catch (IllegalAccessException ile) {
                System.err.println("[sjsql] Driver: Internal Error: " + ile.toString());
                driverError = ile.toString();
            } catch (ClassNotFoundException ce) {
                System.err.println("[sjsql] Driver not found: " + ce.toString());
                driverError = ce.toString();
            } catch (InstantiationException ine) {
                System.err.println("[sjsql] Driver found but could not initialize: " + ine.toString());
                driverError = ine.toString();
            }
        }
    }

    public ResultSet execute(String sql) throws SQLException {

        // Initialise and output top of page
        ResultSet r = null;

        if(initialisedOk) {

            // Create a statement and execute the query on it
            if(c != null) {

                // Get result set
                if(s != null) {
                       s.execute(sql);
                    r = s.getResultSet();
                } else
                    throw new SQLException("[sjsql] Could not execute: Query returned null");
            } else
                throw new SQLException("[sjsql] Could not execute: getConnection returned null");

        } else
            throw new SQLException("[sjsql] Driver not initialised: Error: "+driverError);

        return(r);
    }

    public ResultSet getNextResultSet() throws SQLException {
		if (s.getMoreResults()) {
        return(s.getResultSet());
		} else {
			return null;
		}
    }

    public void cleanup() throws SQLException {

        // Clean up
        try {
            if(s != null)    {    s.close(); s = null; }
        } catch(Exception e) {} catch(Error e) {}
        try {
            if(c != null)    {    c.close(); c = null; }
        } catch(Exception e) {} catch(Error e) {}
    }

    public void startTransaction() throws SQLException {
        c.setAutoCommit(false);
    }

    public void commit() throws SQLException {
        c.commit();
    }

    public void rollback() throws SQLException {
        c.rollback();
    }
}
