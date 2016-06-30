/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package loader;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import writer.DMTargetWritter;

/**
 *
 * @author baradp
 */
public class DMVerticaConnector {

    public static void main(String args[]) {
//        DMVerticaConnector.runCopyCommand("//16.181.234.177", "5433", "VMart", "dbadmin", "dbadmin$$", "~", "\n", "/home/dbadmin/DataMiner/dist/out/[Clients-USER]20140827224127", "TWITTER_USER");
        DMVerticaConnector.runCustomCommand("vsql -d Vmart -f /home/dbadmin/DataMiner/sql/sentiment_calculation_dummy.sql -o /home/dbadmin/DataMiner/sql/sentiment_calculation_dummy.out -h 16.181.234.177 -p 5433 -U dbadmin -w dbadmin$$");

    }

    public static LinkedHashMap<String, LinkedList<String>> getPopularKeywordsMap(String hostName, String port, String dbName, String userName, String password, String query, LinkedList<String> userList, HashMap<String, Integer> patternToRecordsMap) {
        LinkedHashMap<String, LinkedList<String>> returnMap = new LinkedHashMap<String, LinkedList<String>>();

        String connectString = "jdbc:vertica:" + hostName + ":" + port + "/" + dbName + "(" + userName + "," + password + ")";
        DMTargetWritter.log("Extracting data from Database:"
                + "  \nConnection details[" + connectString + "]");

        try {

            Class.forName("com.vertica.jdbc.Driver");

        } catch (ClassNotFoundException e) {

            DMTargetWritter.log("Could not find the JDBC driver class.");

            DMTargetWritter.log(e);

            return returnMap;

        }

        Connection conn = null;

        try {

            conn = DriverManager.getConnection(
                    "jdbc:vertica:" + hostName + ":" + port + "/" + dbName, userName, password);
            for (String orgUser : userList) {
                Statement stmt = conn.createStatement();
                query = query.replace("[statusOriginalUserFilter]", orgUser);
                DMTargetWritter.log("\nQuery to be run: " + query);
                ResultSet resultSet = stmt.executeQuery(query);
                while (resultSet.next()) {
                    if (userList.contains(orgUser)) {
                        String derUser = resultSet.getString("statusDerivedUserFilter");
                        String hashTag = resultSet.getString("statusHashtagEntity");
                        Integer records = resultSet.getInt("RECORD_COUNT");
//                        LinkedList<String> keywords = new LinkedList<String>();
//                        keywords.add(derUser);
//                        keywords.add(hashTag);

                        patternToRecordsMap.put(derUser + "+" + hashTag, records);
                        if (returnMap.get(orgUser) == null) {
                            returnMap.put(orgUser, new LinkedList<String>());
                        }
                        returnMap.get(orgUser).add(derUser + "+" + hashTag);

                    }
                }
                query = query.replace(orgUser, "[statusOriginalUserFilter]");

            }
            conn.close();

        } catch (Exception e) {
            try {
                conn.close();
            } catch (SQLException ex) {
                DMTargetWritter.log("Error while closing connection: ");

                DMTargetWritter.log(ex);
            }

            DMTargetWritter.log("Loading error: ");

            DMTargetWritter.log(e);

        } finally {
            try {
                conn.close();
            } catch (SQLException ex) {
                DMTargetWritter.log("Error while closing connection: ");
                DMTargetWritter.log(ex);
            }

        }

        return returnMap;
    }

    public static boolean runCopyCommand(String hostName, String port, String dbName, String userName, String password, String delimiter, String lineSeparator, String fileName, String tableName) {

        boolean returnCode = false;
        delimiter = delimiter.replace("\n", "\\n").replace("\r", "\\r");
        lineSeparator = lineSeparator.replace("\n", "\\n").replace("\r", "\\r");
        String copyCommand = "COPY " + tableName + " FROM '" + fileName + "' DELIMITER '" + delimiter + "' ENCLOSED BY '\"' RECORD TERMINATOR E'" + lineSeparator + "' SKIP 1 DIRECT ENFORCELENGTH";
        String connectString = "jdbc:vertica:" + hostName + ":" + port + "/" + dbName + "(" + userName + "," + password + ")";
        DMTargetWritter.log("Loading data from file:" + fileName + "  to table:" + tableName
                + "  \nConnection details==>[" + connectString + "]" + "\nCommand to be executed:" + copyCommand);

        try {

            Class.forName("com.vertica.jdbc.Driver");

        } catch (ClassNotFoundException e) {

            DMTargetWritter.log("Could not find the JDBC driver class.");

            DMTargetWritter.log(e);

            return returnCode;

        }

        Connection conn = null;

        try {

            conn = DriverManager.getConnection(
                    "jdbc:vertica:" + hostName + ":" + port + "/" + dbName, userName, password);

            Statement stmt = conn.createStatement();

            returnCode = stmt.execute(copyCommand);
            if (!returnCode) {
                // Count will usually return the count of rows inserted.
                int rowCount = stmt.getUpdateCount();
                DMTargetWritter.log("File:" + fileName + " -->Number of accepted rows = " + rowCount);
                if (rowCount > 0) {
                    returnCode = true;
                }
            }
            conn.close();
        } catch (Exception e) {
            try {
                conn.close();
            } catch (SQLException ex) {
                DMTargetWritter.log("Error while closing connection: ");

                DMTargetWritter.log(ex);
            }

            DMTargetWritter.log("Loading error: ");

            DMTargetWritter.log(e);

        }

        return returnCode;
    }

    public synchronized static void runCustomCommand(String command) {

        try {
            DMTargetWritter.log("Executing custom command:" + command);
            StringTokenizer commandsToken = new StringTokenizer(command, " ");
            String[] commandExecutable = new String[commandsToken.countTokens()];
            int i = 0;
            while (commandsToken.hasMoreTokens()) {
                commandExecutable[i] = commandsToken.nextToken();
                i++;
            }
            Runtime.getRuntime().exec(commandExecutable);
        } catch (IOException ex) {
            DMTargetWritter.log(ex);
        }

    }

}
