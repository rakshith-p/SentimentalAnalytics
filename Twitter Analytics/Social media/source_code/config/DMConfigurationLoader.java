/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author baradp
 */
public class DMConfigurationLoader {

    public static List<LinkedHashMap<String, String>> connectionPool = new ArrayList<LinkedHashMap<String, String>>();
    public static List<LinkedHashMap<String, String>> batchConfigList = new ArrayList<LinkedHashMap<String, String>>();
    public static List<String> batchFileList = new ArrayList<String>();
    public static String outputDirectory = null;
    public static String directorySeparator;
    public static String fieldSeparator;
    public static String custCommandIntervalMin;
    public static boolean allowParallelDBLoading;
    public static String dbSyncIntervalMin;

    public static String lineSeparator;

    public static long applicationLifeTimeLong;
    public static long shutdownTime;
    public static String customCommand;

    public static int writerBufferSize = 256;
    public static String dbHost;
    public static String dbPort;
    public static String dbName;
    public static String dbUsername;
    public static String dbPassword;
    public static String twitterUserTable;
    public static String twitterStatusTable;

    public static int proxyPort;
    public static String proxyHost;
    public static String proxyUser;
    public static String proxyPassword;
    public static JSONObject emailConfigObject;

    public static void init(File appConfigFie) throws IOException, ParseException {
        loadAppConfigs(appConfigFie);
        loadBatchConfigs();
    }

    private static void loadAppConfigs(File appConfigFie) throws IOException, ParseException {
//            metadataFileName  = fileName;

        /**
         * Reading Application Configuration File
         *
         */
        JSONObject appConfigObject = (JSONObject) new JSONParser().parse(new FileReader(appConfigFie));
        /**
         * Reading API keys details
         *
         */
        JSONArray sections = (JSONArray) appConfigObject.get(DMConfigurationConstants.APP_CONNECTION_POOL);
        Iterator<JSONObject> connIterator = (Iterator<JSONObject>) sections.iterator();

        while (connIterator.hasNext()) {
            JSONObject connObject = connIterator.next();
            LinkedHashMap<String, String> conn = new LinkedHashMap<String, String>();
            conn.putAll(connObject);
            connectionPool.add(conn);
        }
        /**
         * Reading Batch configuration
         *
         */
        List<String> batchFilesBuffer = (List<String>) appConfigObject.get(DMConfigurationConstants.APP_BATCH_FILE_LIST);
        DMConfigurationLoader.batchFileList.addAll(batchFilesBuffer);
        DMConfigurationLoader.outputDirectory = (String) appConfigObject.get(DMConfigurationConstants.APP_OUTPUT_DIR);
        DMConfigurationLoader.fieldSeparator = (String) appConfigObject.get(DMConfigurationConstants.APP_FIELD_SEPARATOR);
        DMConfigurationLoader.dbSyncIntervalMin = (String) appConfigObject.get(DMConfigurationConstants.APP_DB_SYNC_INTERVAL);
        DMConfigurationLoader.custCommandIntervalMin = (String) appConfigObject.get(DMConfigurationConstants.APP_CUST_COMMANDL_EXEC_INTERVAL);
        String allow = (String) appConfigObject.get(DMConfigurationConstants.APP_ALLOW_PARALLEL_DB_LOADING);
        DMConfigurationLoader.allowParallelDBLoading = Boolean.valueOf(allow);

        String shutdown = (String) appConfigObject.get(DMConfigurationConstants.APP_SHUTDOWN_TIME);
        DMConfigurationLoader.customCommand = (String) appConfigObject.get(DMConfigurationConstants.APP_CUSTOM_COMMAND);
        if (shutdown != null) {
            DMConfigurationLoader.applicationLifeTimeLong = Long.valueOf(shutdown) * 60 * 1000;
        } else {
            DMConfigurationLoader.applicationLifeTimeLong = 30 * 24 * 60 * 60 * 1000; //One months
        }
        DMConfigurationLoader.shutdownTime = System.currentTimeMillis() + DMConfigurationLoader.applicationLifeTimeLong; //One months

        DMConfigurationLoader.lineSeparator = (String) appConfigObject.get(DMConfigurationConstants.APP_LINE_SEPARATOR);
        DMConfigurationLoader.dbHost = (String) appConfigObject.get(DMConfigurationConstants.APP_DB_HOST);
        DMConfigurationLoader.dbName = (String) appConfigObject.get(DMConfigurationConstants.APP_DB_NAME);
        DMConfigurationLoader.dbPassword = (String) appConfigObject.get(DMConfigurationConstants.APP_DB_PASSWORD);
        DMConfigurationLoader.dbPort = (String) appConfigObject.get(DMConfigurationConstants.APP_DB_PORT);
        DMConfigurationLoader.dbUsername = (String) appConfigObject.get(DMConfigurationConstants.APP_DB_USERNAME);
        DMConfigurationLoader.twitterStatusTable = (String) appConfigObject.get(DMConfigurationConstants.APP_TWITTER_STATUS_TABLE);
        DMConfigurationLoader.twitterUserTable = (String) appConfigObject.get(DMConfigurationConstants.APP_TWITTER_USER_TABLE);

        DMConfigurationLoader.proxyHost = (String) appConfigObject.get(DMConfigurationConstants.APP_PROXY_HOST);
        DMConfigurationLoader.proxyUser = (String) appConfigObject.get(DMConfigurationConstants.APP_PROXY_USER);
        DMConfigurationLoader.proxyPassword = (String) appConfigObject.get(DMConfigurationConstants.APP_PROXY_PASSWORD);
        String portString = (String) appConfigObject.get(DMConfigurationConstants.APP_PROXY_PORT);
        if (portString != null && portString.length() > 0) {
            DMConfigurationLoader.proxyPort = Integer.valueOf(portString);
        } else {
            DMConfigurationLoader.proxyPort = -1;
        }

        DMConfigurationLoader.writerBufferSize = Integer.valueOf((String) appConfigObject.get(DMConfigurationConstants.APP_WRITTER_BUFFER));

        DMConfigurationLoader.directorySeparator = (String) appConfigObject.get(DMConfigurationConstants.APP_DIR_SEPARATOR);
        DMConfigurationLoader.outputDirectory = new File(DMConfigurationLoader.outputDirectory).getCanonicalPath() + DMConfigurationLoader.directorySeparator;

        //Loading Email configurations
        
         emailConfigObject = (JSONObject)appConfigObject.get(DMConfigurationConstants.APP_EMAIL_CONFIGURATION);
    }

    private static void loadBatchConfigs() throws FileNotFoundException, IOException, ParseException {
        for (String batchFile : DMConfigurationLoader.batchFileList) {
            JSONObject batchObj = (JSONObject) new JSONParser().parse(new FileReader(batchFile));
            LinkedHashMap<String, String> batchMap = new LinkedHashMap<String, String>();
            batchMap.putAll(batchObj);
            DMConfigurationLoader.batchConfigList.add(batchMap);

        }
    }

}
