/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package engine;

import alerts.DMEmailSender;
import batch.DMBatchProcessor;
import batch.DMStatusListner;
import config.DMConfigurationConstants;
import config.DMConfigurationLoader;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.json.simple.parser.ParseException;
import twitter4j.FilterQuery;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import writer.DMTargetWritter;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import loader.DMVerticaConnector;
import sun.awt.windows.ThemeReader;

/**
 *
 * @author baradp
 */
public class DMController {

//    public static String authFileName = "";
    private static int connectionBatchIndex = -1;
    private static int connectionStreamIndex = -1;

//    private static int batchIndex = -1;
    private static int maxBatchConnectionCount = -1;
    private static int maxStreamConnectionCount = -1;

    private static int maxBatchCount = -1;

//    public static List<TwitterStream> twitterStreamConnectionPool = new ArrayList<TwitterStream>();
    public static List<Twitter> twitterConnectionPool = new ArrayList<Twitter>();
    public static ScheduledExecutorService scheduler;
    public static Map<String, Long> userFilterToCutOffMinDateLong = new HashMap<String, Long>();
    public static Map<String, Long> userNameToPrevSinceIdMap = new HashMap<String, Long>();
    public static Map<String, Long> userNameToPrevMaxIdMap = new HashMap<String, Long>();
    public static Map<String, String> userNameToPagingModeMap = new HashMap<String, String>();
//    public static Map<String, String> batchNameToTrackStreamMap = new LinkedHashMap<String, String>();
//    public static LinkedList<String> outputFiles = new LinkedList<String>();
    public static LinkedHashMap<String, LinkedList<String>> orgUserToPopularKeywordsMap = new LinkedHashMap<String, LinkedList<String>>();
//    public static Map<String, LinkedList<String>> userNameToUserFilesMap = new HashMap<String, LinkedList<String>>();
    public static Map<String, LinkedList<String>> batchNameToInquiredUserListMap = new HashMap<String, LinkedList<String>>();
    public static Map<String, LinkedList<String>> batchNameToRegisteredUserListMap = new HashMap<String, LinkedList<String>>();
    public static Map<String, String> derToOrgUserNameMap = new HashMap<String, String>();

//    public static Map<String, LinkedList<String>> userNameToStatusFilesMap = new HashMap<String, LinkedList<String>>();
//    public static Map<String, LinkedList<String>> userNameToUserFilesMap = new HashMap<String, LinkedList<String>>();
//    public static Map<String, LinkedList<String>> userNameToStatusFilesMap = new HashMap<String, LinkedList<String>>();
    private static Map<String, ScheduledFuture> batchNameToSchedulerInstanceMap = new LinkedHashMap<String, ScheduledFuture>();
    public static Map<String, String> batchNameToSwitchModeMap = new LinkedHashMap<String, String>();

    public static boolean hasTrackerStarted = false;
    public static Map<String, Long> twitterRegisteredUserScreenNameToIdMap = new LinkedHashMap<String, Long>();
    /**
     * *
     *
     * The following variable is batch schedule period and recommended value for
     * it is 20
     */
    public static final int BATCH_SCHEDULE_PERIOD_MINUTE = 5;

//    public static List<Long> referenceUserIds = new LinkedList<Long>();
//    public static List<Long> userIds = new LinkedList<Long>();
    public static ArrayList<String> processedUsers = new ArrayList<String>();
    public static ArrayList<String> processedStatuses = new ArrayList<String>();
    public static final String PROCESSED_USER_FILE = "processedUsers";
    public static final String PROCESSED_STATUS_FILE = "processedStatuses";
    //File name constants
    public static final String TWITTER_REGISTERED_USER_SCREEN_NAME_TO_ID_MAP_FILE = "twitterRegisteredUserScreenNameToIdMap";
    public static final String BATCH_NAME_TO_REGISTERED_USER_LIST_MAP_FILE = "batchNameToRegisteredUserListMap";
    public static final String USER_FILTER_TO_CUT_OFF_MIN_DATE_LONG_FILE = "userFilterToCutOffMinDateLong";
    public static final String DER_USER_TO_ORG_USER_NAME_MAP_FILE = "derToOrgUserNameMap";
    public static final String PROCESSED_USER_FILE_COUNT_FILE = "processedStatusFileCount";
    public static final String PROCESSED_STATUS_FILE_COUNT_FILE = "processedUserFileCount";

//    public static final String USERNAME_TO_PREV_SINCE_ID_MAP_FILE = "userNameToPrevSinceIdMap";
//    public static final String USERNAME_TO_PREV_MAX_ID_MAP_FILE = "userNameToPrevMaxIdMap";
//    public static final String USERNAME_TO_PAGING_MODE_MAP_FILE = "userNameToPagingModeMap";
    public static Integer processedStatusFileCount = 0;
    public static Integer processedUserFileCount = 0;
//
    public static final String DEFAULT_APP_FILE = "/config/appConfig.json";
    public static final String DEFAULT_BATCH_FILE1 = "/config/Clients.json";
    public static final String DEFAULT_BATCH_FILE2 = "/config/NewClients.json";
    public static final String DEFAULT_BATCH_FILE3 = "/config/NewsChannels.json";

    public static final HashMap<String, Integer> patternToRecordCountMap = new HashMap<String, Integer>();
    public static String dbQuery;
    public static Boolean dbPatternReaderLock = false;

    public DMController(String appConfigFileName) throws IOException, ParseException {

        File appConfigFile = writeDefaultFile(DEFAULT_APP_FILE);
        writeDefaultFile(DEFAULT_BATCH_FILE1);
        writeDefaultFile(DEFAULT_BATCH_FILE2);
        writeDefaultFile(DEFAULT_BATCH_FILE3);

        loadConfigs(appConfigFile);
        maxBatchConnectionCount = DMConfigurationLoader.connectionPool.size();
        maxStreamConnectionCount = DMConfigurationLoader.connectionPool.size();
        maxBatchCount = DMConfigurationLoader.batchConfigList.size();
        dbQuery = "SELECT statusDerivedUserFilter, statusHashtagEntity, Count(*) AS RECORD_COUNT "
                + "FROM " + DMConfigurationLoader.twitterStatusTable + ""
                + " WHERE statusHashtagEntity <> 'null' "
                + "and statusOriginalUserFilter='[statusOriginalUserFilter]' "
                + " and statusFilterType <> 'STREAM_TRACKER' "
                + "GROUP BY statusDerivedUserFilter,statusOriginalUserFilter, statusHashtagEntity "
                + "ORDER BY record_count DESC LIMIT 5";
        //Initialize target writer object
        DMTargetWritter.init(DMConfigurationLoader.writerBufferSize, DMConfigurationLoader.fieldSeparator, DMConfigurationLoader.lineSeparator, Long.valueOf(DMConfigurationLoader.dbSyncIntervalMin), Long.valueOf(DMConfigurationLoader.custCommandIntervalMin));

        File file = new File(DMConfigurationLoader.outputDirectory);
        if (!file.isDirectory()) {
            file = new File("out/");
        }
        if (!file.exists()) {
            file.mkdirs();
        }
        try {
            deserialize();
        } catch (FileNotFoundException ex) {
            DMTargetWritter.log(ex);
        } catch (ClassNotFoundException ex) {
            DMTargetWritter.log(ex);
        }
        //Create Twitter Connection Pool
        this.createConnectionPool();
//        this.createStreamConnectionPool();

        if (maxBatchCount == 0) {
            DMTargetWritter.log("Unable to find Batch to be executed!!!\n\nDid you specify the \"batchFileList\" list in <Appliaction Configuration File>");
        }
        if (maxBatchConnectionCount == 0) {
            DMTargetWritter.log("Unable to find Appliaction Configuration detais for Twitter!!!\n\nDid you specify the valid API keys and Proxy sever details to connect to Twiiter API in <Appliaction Configuration File>");
        }
        if (maxBatchConnectionCount < maxBatchCount) {
            DMTargetWritter.log("Performance Warning: Number of Batch to Twitter API Connection ratio is:\n" + maxBatchCount + ":" + maxBatchConnectionCount);
        }
        connectionBatchIndex = 0;
        connectionStreamIndex = 0;
        scheduler = Executors.newScheduledThreadPool(maxBatchCount);
//        batchIndex = 0;

    }

    public static synchronized void serialize() throws IOException {
        File file = new File("serial/");
        DMTargetWritter.serialDir = "serial/";

        if (!file.exists()) {
            file.mkdirs();

        } else {
            ObjectOutputStream outStream = null;
            File objectFile = null;
            if (twitterRegisteredUserScreenNameToIdMap != null && !twitterRegisteredUserScreenNameToIdMap.isEmpty()) {
                objectFile = new File(DMTargetWritter.serialDir + DMController.TWITTER_REGISTERED_USER_SCREEN_NAME_TO_ID_MAP_FILE);
                if (objectFile != null) {
                    outStream = new ObjectOutputStream(new FileOutputStream(objectFile));
                    outStream.writeObject(DMController.twitterRegisteredUserScreenNameToIdMap);

                    outStream.close();
                }
            }
//            if (userFilterToCutOffMinDateLong != null && !userFilterToCutOffMinDateLong.isEmpty()) {
//                objectFile = new File(DMTargetWritter.serialDir + DMController.USER_FILTER_TO_CUT_OFF_MIN_DATE_LONG_FILE);
//                if (objectFile != null) {
//                    outStream = new ObjectOutputStream(new FileOutputStream(objectFile));
//                    outStream.writeObject(DMController.userFilterToCutOffMinDateLong);
//                    outStream.close();
//                }
//            }
            if (processedStatusFileCount != null && processedStatusFileCount.intValue() > 0) {
                objectFile = new File(DMTargetWritter.serialDir + DMController.PROCESSED_STATUS_FILE_COUNT_FILE);
                if (objectFile != null) {
                    outStream = new ObjectOutputStream(new FileOutputStream(objectFile));
                    outStream.writeObject(DMController.processedStatusFileCount);
                    outStream.close();
                }
            }
            if (processedUserFileCount != null && processedUserFileCount.intValue() > 0) {
                objectFile = new File(DMTargetWritter.serialDir + DMController.PROCESSED_USER_FILE_COUNT_FILE);
                if (objectFile != null) {
                    outStream = new ObjectOutputStream(new FileOutputStream(objectFile));
                    outStream.writeObject(DMController.processedUserFileCount);
                    outStream.close();
                }
            }
            if (batchNameToRegisteredUserListMap != null && !batchNameToRegisteredUserListMap.isEmpty()) {
                objectFile = new File(DMTargetWritter.serialDir + DMController.BATCH_NAME_TO_REGISTERED_USER_LIST_MAP_FILE);
                if (objectFile != null) {
                    outStream = new ObjectOutputStream(new FileOutputStream(objectFile));
                    outStream.writeObject(DMController.batchNameToRegisteredUserListMap);
                    outStream.close();
                }
            }
            if (derToOrgUserNameMap != null && !derToOrgUserNameMap.isEmpty()) {
                objectFile = new File(DMTargetWritter.serialDir + DMController.DER_USER_TO_ORG_USER_NAME_MAP_FILE);
                if (objectFile != null) {
                    outStream = new ObjectOutputStream(new FileOutputStream(objectFile));
                    outStream.writeObject(DMController.derToOrgUserNameMap);
                    outStream.close();
                }
            }

        }
    }

    public static synchronized void deserialize() throws FileNotFoundException, IOException, ClassNotFoundException {
        File file = new File("serial/");
        DMTargetWritter.serialDir = "serial/";

        if (!file.exists()) {
            file.mkdirs();

        } else {
            ObjectInputStream inStream = null;
            File objectFile = new File(DMTargetWritter.serialDir + DMController.TWITTER_REGISTERED_USER_SCREEN_NAME_TO_ID_MAP_FILE);
            if (objectFile != null && objectFile.isFile() && objectFile.exists()) {
                inStream = new ObjectInputStream(new FileInputStream(objectFile));
                twitterRegisteredUserScreenNameToIdMap = (LinkedHashMap<String, Long>) (inStream.readObject());
                if (twitterRegisteredUserScreenNameToIdMap == null) {
                    twitterRegisteredUserScreenNameToIdMap = new LinkedHashMap<String, Long>();
                }
                inStream.close();
            }
//            objectFile = new File(DMTargetWritter.serialDir + DMController.USER_FILTER_TO_CUT_OFF_MIN_DATE_LONG_FILE);
//            if (objectFile != null && objectFile.isFile() && objectFile.exists()) {
//                inStream = new ObjectInputStream(new FileInputStream(objectFile));
//                userFilterToCutOffMinDateLong = (HashMap<String, Long>) (inStream.readObject());
//                if (userFilterToCutOffMinDateLong == null) {
//                    userFilterToCutOffMinDateLong = new HashMap<String, Long>();
//                }
//                inStream.close();
//            }
            objectFile = new File(DMTargetWritter.serialDir + DMController.PROCESSED_STATUS_FILE_COUNT_FILE);
            if (objectFile != null && objectFile.isFile() && objectFile.exists()) {
                inStream = new ObjectInputStream(new FileInputStream(objectFile));
                processedStatusFileCount = (Integer) (inStream.readObject());
                if (processedStatusFileCount == null) {
                    processedStatusFileCount = 0;
                }
                inStream.close();
            }
            objectFile = new File(DMTargetWritter.serialDir + DMController.PROCESSED_USER_FILE_COUNT_FILE);
            if (objectFile != null && objectFile.isFile() && objectFile.exists()) {
                inStream = new ObjectInputStream(new FileInputStream(objectFile));
                processedUserFileCount = (Integer) (inStream.readObject());
                if (processedUserFileCount == null) {
                    processedUserFileCount = 0;
                }
                inStream.close();
            }
            objectFile = new File(DMTargetWritter.serialDir + DMController.BATCH_NAME_TO_REGISTERED_USER_LIST_MAP_FILE);
            if (objectFile != null && objectFile.isFile() && objectFile.exists()) {
                inStream = new ObjectInputStream(new FileInputStream(objectFile));
                batchNameToRegisteredUserListMap = (HashMap<String, LinkedList<String>>) (inStream.readObject());
                if (batchNameToRegisteredUserListMap == null) {
                    batchNameToRegisteredUserListMap = new HashMap<String, LinkedList<String>>();
                }
                inStream.close();
            }
            objectFile = new File(DMTargetWritter.serialDir + DMController.DER_USER_TO_ORG_USER_NAME_MAP_FILE);
            if (objectFile != null && objectFile.isFile() && objectFile.exists()) {
                inStream = new ObjectInputStream(new FileInputStream(objectFile));
                derToOrgUserNameMap = (HashMap<String, String>) (inStream.readObject());
                if (derToOrgUserNameMap == null) {
                    derToOrgUserNameMap = new HashMap<String, String>();
                }
                inStream.close();
            }

        }
    }

    public static synchronized void unscheduleFwdPageBatches() {

        for (Map.Entry<String, LinkedList<String>> entry : batchNameToInquiredUserListMap.entrySet()) {
            boolean unscheduleBatch = true;
            for (String user : entry.getValue()) {
                if (userNameToPagingModeMap.get(user) == null || userNameToPagingModeMap.get(user).equals(DMBatchProcessor.REVERSE_PAGINATION)) {
                    unscheduleBatch = false;
                }
            }
            if (unscheduleBatch) {
                ScheduledFuture schedulerInstance = batchNameToSchedulerInstanceMap.get(entry.getKey());
                schedulerInstance.cancel(true);
                if (batchNameToSwitchModeMap.get(entry.getKey()) == null) {
                    batchNameToSwitchModeMap.put(entry.getKey(), "true");
                    DMTargetWritter.log("Unscheduled Search API connection for [" + entry.getKey() + "].(Reverse Pagination completed for this batch)");

                    scheduleStream(entry.getKey());

                }
            }

        }
    }

    public static synchronized void scheduleStream(String batchName) {
        scheduleStreamToFollow(null, batchName);
        scheduleStreamToTrack(null, batchName);
    }

    public synchronized static boolean lookupCacheFileStatus(String element) throws NullPointerException {
        if (DMController.processedStatuses.contains(element)) {
            return true;
        } else {
            for (int index = DMController.processedStatusFileCount - 1; index >= 0; index--) {
                ObjectInputStream inStream = null;
                try {
                    File file = new File(DMTargetWritter.serialDir + DMController.PROCESSED_STATUS_FILE + index);
                    if (file.exists()) {
                        inStream = new ObjectInputStream(new FileInputStream(file));
                        DMController.processedUsers = (ArrayList<String>) (inStream.readObject());
                        return (DMController.processedUsers).contains(element);
                    } else {
                        return false;
                    }

                } catch (ClassNotFoundException ex) {
                    DMTargetWritter.log(ex);
                } catch (FileNotFoundException ex) {
                    DMTargetWritter.log(ex);
                } catch (IOException ex) {
                    DMTargetWritter.log(ex);
                } finally {
                    try {
                        inStream.close();
                    } catch (IOException ex) {
                        DMTargetWritter.log(ex);
                    }
                }
            }

            DMController.processedStatuses.add(element);
            if (DMController.processedStatuses.size() > 100000) {
                serializeProcessedStatuses();
            }
            return false;

        }
    }

    public synchronized static void serializeProcessedStatuses() {
        ObjectOutputStream outStream = null;

        try {
            DMTargetWritter.log("Processed Statuses Count= " + DMController.processedStatuses.size());
            outStream = new ObjectOutputStream(new FileOutputStream(DMTargetWritter.serialDir + DMController.PROCESSED_STATUS_FILE + DMController.processedStatusFileCount));
            outStream.writeObject(DMController.processedStatuses);
            DMController.processedStatusFileCount++;

        } catch (IOException ex) {
            DMTargetWritter.log(ex);
        } finally {
            try {
                outStream.close();
            } catch (IOException ex) {
                DMTargetWritter.log(ex);
            }
        }
        DMController.processedStatuses.clear();
    }

    public synchronized static boolean lookupCacheFileUser(String element) throws NullPointerException {
        if (DMController.processedUsers.contains(element)) {
            return true;
        } else {
            for (int index = DMController.processedUserFileCount - 1; index >= 0; index--) {
                ObjectInputStream inStream = null;
                try {
                    File file = new File(DMTargetWritter.serialDir + DMController.PROCESSED_USER_FILE + index);
                    if (file.exists()) {
                        inStream = new ObjectInputStream(new FileInputStream(file));
                        DMController.processedUsers = (ArrayList<String>) (inStream.readObject());
                        return (DMController.processedUsers).contains(element);
                    } else {
                        return false;
                    }

                } catch (ClassNotFoundException ex) {
                    DMTargetWritter.log(ex);
                } catch (FileNotFoundException ex) {
                    DMTargetWritter.log(ex);
                } catch (IOException ex) {
                    DMTargetWritter.log(ex);
                } finally {
                    try {
                        inStream.close();
                    } catch (IOException ex) {
                        DMTargetWritter.log(ex);
                    }
                }
            }

            DMController.processedUsers.add(element);
            if (DMController.processedUsers.size() > 100000) {
                serializeProcessedUsers();
            }
            return false;

        }
    }

    public synchronized static void serializeProcessedUsers() {
        ObjectOutputStream outStream = null;

        try {
            DMTargetWritter.log("Processed Users Count= " + DMController.processedUsers.size());

            outStream = new ObjectOutputStream(new FileOutputStream(DMTargetWritter.serialDir + DMController.PROCESSED_USER_FILE + DMController.processedUserFileCount));
            outStream.writeObject(DMController.processedUsers);
            DMController.processedUserFileCount++;

        } catch (IOException ex) {
            DMTargetWritter.log(ex);
        } finally {
            try {
                outStream.close();
            } catch (IOException ex) {
                DMTargetWritter.log(ex);
            }
        }
        DMController.processedUsers.clear();
    }

    public static File writeDefaultFile(String resourceName) throws IOException {
        File newFile = null;
        BufferedReader reader = new BufferedReader(new InputStreamReader(DMController.class.getResourceAsStream(resourceName)));
        String newFileName = resourceName.substring(1);
        newFile = new File(newFileName).getCanonicalFile();
        if (!newFile.exists()) {

            newFile.getParentFile().mkdirs();
            BufferedWriter writer = new BufferedWriter(new FileWriter(newFile));
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.write("\n");
            }
            writer.close();
        }
        return newFile;
    }

    private void loadConfigs(File appConfigFile) throws IOException, ParseException {
        DMConfigurationLoader.init(appConfigFile);
        DMEmailSender.init(DMConfigurationLoader.emailConfigObject);
    }

    private void createConnectionPool() {
        for (LinkedHashMap connPool : DMConfigurationLoader.connectionPool) {
            String consumerKey = (String) connPool.get(DMConfigurationConstants.APP_CONSUMER_KEY);
            String consumerKeySecret = (String) connPool.get(DMConfigurationConstants.APP_CONSUMER_KEY_SECRET);
            String accessToken = (String) connPool.get(DMConfigurationConstants.APP_ACCESS_TOKEN);
            String accessTokenSecret = (String) connPool.get(DMConfigurationConstants.APP_ACCESS_TOKEN_SECRET);
            ConfigurationBuilder configBuilder = new ConfigurationBuilder();

            configBuilder.setOAuthConsumerKey(consumerKey);
            configBuilder.setOAuthConsumerSecret(consumerKeySecret);
            configBuilder.setOAuthAccessToken(accessToken);
            configBuilder.setOAuthAccessTokenSecret(accessTokenSecret);
            if (DMConfigurationLoader.proxyPort != -1) {
                configBuilder.setHttpProxyHost(DMConfigurationLoader.proxyHost);
                configBuilder.setHttpProxyPort(DMConfigurationLoader.proxyPort);
                configBuilder.setHttpProxyUser(DMConfigurationLoader.proxyUser);
                configBuilder.setHttpProxyPassword(DMConfigurationLoader.proxyPassword);
            }
            Twitter twitter = new TwitterFactory(configBuilder.build()).getInstance();
            DMController.twitterConnectionPool.add(twitter);
        }
    }

    private static synchronized TwitterStream createStreamConnection(int index) {
        LinkedHashMap connPool = DMConfigurationLoader.connectionPool.get(index);
        String consumerKey = (String) connPool.get(DMConfigurationConstants.APP_CONSUMER_KEY);
        String consumerKeySecret = (String) connPool.get(DMConfigurationConstants.APP_CONSUMER_KEY_SECRET);
        String accessToken = (String) connPool.get(DMConfigurationConstants.APP_ACCESS_TOKEN);
        String accessTokenSecret = (String) connPool.get(DMConfigurationConstants.APP_ACCESS_TOKEN_SECRET);
        ConfigurationBuilder configBuilder = new ConfigurationBuilder();

        configBuilder.setOAuthConsumerKey(consumerKey);
        configBuilder.setOAuthConsumerSecret(consumerKeySecret);
        configBuilder.setOAuthAccessToken(accessToken);
        configBuilder.setOAuthAccessTokenSecret(accessTokenSecret);
        if (DMConfigurationLoader.proxyPort != -1) {
            configBuilder.setHttpProxyHost(DMConfigurationLoader.proxyHost);
            configBuilder.setHttpProxyPort(DMConfigurationLoader.proxyPort);
            configBuilder.setHttpProxyUser(DMConfigurationLoader.proxyUser);
            configBuilder.setHttpProxyPassword(DMConfigurationLoader.proxyPassword);
        }
        TwitterStream twitter = new TwitterStreamFactory(configBuilder.build()).getInstance();
        return twitter;
    }

    public static void releaseStreamConnection(TwitterStream twitter) {

        twitter.clearListeners();
        twitter.cleanUp();
        twitter.shutdown();
//        twitter.setOAuthAccessToken(null);
//        twitter.setOAuthConsumer(null, null);
        DMController.connectionStreamIndex--;
    }

    private static TwitterStream pullStreamConnection() {
        if (DMController.connectionStreamIndex < 0 || DMController.connectionStreamIndex >= DMController.maxStreamConnectionCount) {
            return null;
        } else {

            TwitterStream twitter = DMController.createStreamConnection(connectionStreamIndex);

            connectionStreamIndex++;
            return twitter;
        }
    }

    private static Twitter pullBatchConnection() {
        if (DMController.connectionBatchIndex < 0 || DMController.connectionBatchIndex >= DMController.maxBatchConnectionCount) {
            return null;
        } else {

            Twitter twitter = DMController.twitterConnectionPool.get(connectionBatchIndex);

            connectionBatchIndex++;
            return twitter;
        }
    }

    private void scheduleBatch() {
        for (int index = 0; index < DMController.maxBatchCount; index++) {
            LinkedHashMap<String, String> batchConfig = DMConfigurationLoader.batchConfigList.get(index);
            Twitter twitter = pullBatchConnection();
            if (twitter != null) {
                String dateFormat = batchConfig.get(DMConfigurationConstants.BATCH_DATE_FORMAT);
                String date = batchConfig.get(DMConfigurationConstants.BATCH_RUN_START_TIME);

                long startTimeLong = 0;

                long initDelayMinute = 0;
                long currTimeLong = System.currentTimeMillis();

                if (date != null && !date.isEmpty()) {
                    try {
                        startTimeLong = new SimpleDateFormat(dateFormat).parse(date).getTime();
                    } catch (java.text.ParseException ex) {
                        DMTargetWritter.log(ex);
                    }
                }

                if (startTimeLong > currTimeLong) {
                    initDelayMinute = (startTimeLong - currTimeLong) / (1000 * 60);
                } else {
                    initDelayMinute = 0;
                }
                try {

                    DMBatchProcessor batchProcessor = new DMBatchProcessor();
                    batchProcessor.init(twitter, batchConfig);

                    ScheduledFuture futureTask = scheduler.scheduleAtFixedRate(batchProcessor, initDelayMinute, BATCH_SCHEDULE_PERIOD_MINUTE, TimeUnit.MINUTES);
                    batchNameToSchedulerInstanceMap.put(batchProcessor.batchName, futureTask);

                    DMTargetWritter.log("INFO: [" + batchProcessor.batchName + "] has been scheduled to run every " + BATCH_SCHEDULE_PERIOD_MINUTE + " minute during Reverse Pagination Mode.");

                } catch (java.text.ParseException ex) {
                    DMTargetWritter.log(ex);
                }

            } else {
                DMTargetWritter.log("FATAL ERROR: [" + batchConfig.get(DMConfigurationConstants.BATCH_NAME) + "] is not scheduled. \n Please add aditional Twitter API credentials to <Application Configuration file> ");

                /**
                 * Logic to handle more batch than twitter connection API
                 *
                 */
            }
        }
        try {
            /**
             * Setup Application shutdown
             *
             */
            new DMEmailSender().start();
            Thread.sleep(DMConfigurationLoader.applicationLifeTimeLong);
        } catch (InterruptedException ex) {
            try {
                serialize();
            } catch (IOException ex1) {
                DMTargetWritter.log(ex1);
            }
            DMTargetWritter.log(ex);

        }
        scheduler.shutdown();

        DMTargetWritter.log("INFO: Application will shutdown after Thread [" + Thread.currentThread().getId() + "] completes!!!");

    }

    private synchronized static LinkedHashMap<String, LinkedList<String>> getPopularKeywordsMap(LinkedList<String> userList) {
        return DMVerticaConnector.getPopularKeywordsMap(DMConfigurationLoader.dbHost, DMConfigurationLoader.dbPort,
                DMConfigurationLoader.dbName, DMConfigurationLoader.dbUsername, DMConfigurationLoader.dbPassword,
                dbQuery, userList, patternToRecordCountMap);
    }

    private static List<String> getKeywords(LinkedHashMap<String, LinkedList<String>> map) {

        List<String> keywords = new LinkedList<String>();
        for (LinkedList<String> list : map.values()) {
            for (String e : list) {
                StringTokenizer token = new StringTokenizer(e, "+");
                keywords.add(token.nextToken());
                keywords.add(token.nextToken());
            }
        }
        return keywords;
    }

    public static LinkedList<LinkedHashMap<String, String>> getNormalizedMap(LinkedHashMap<String, String> paramMap, LinkedList<String> paramList, String paramPropertyName) {
        LinkedList<LinkedHashMap<String, String>> bufferMapList = new LinkedList<LinkedHashMap<String, String>>();
//        if (paramList.size() == 0) {
//            bufferMapList.add(paramMap);
//            return bufferMapList;
//        }
//        for (String element : paramList) {
//            paramMap.put(paramPropertyName, element);
//            bufferMapList.add(paramMap);
//        }
//        return bufferMapList;
        switch (paramList.size()) {
            case 0:
                paramMap.put(paramPropertyName, null);

                bufferMapList.add(paramMap);
                return bufferMapList;
            case 1:
                paramMap.put(paramPropertyName, paramList.get(0));
                bufferMapList.add(paramMap);
                return bufferMapList;

            default:
                for (String element : paramList) {
                    LinkedHashMap<String, String> bufferMap = new LinkedHashMap<String, String>();
                    bufferMap.putAll(paramMap);
                    bufferMap.put(paramPropertyName, element);
                    bufferMapList.add(bufferMap);
                }

                return bufferMapList;

        }
    }

    private static synchronized void scheduleStreamToFollow(Exception e, String paramBatchName) {
//        List<LinkedHashMap<String, String>> batchConfigListBuffer = new ArrayList<LinkedHashMap<String, String>>();

//        batchConfigListBuffer.addAll(DMConfigurationLoader.batchConfigList);
        try {
            for (LinkedHashMap<String, String> batchConfig : DMConfigurationLoader.batchConfigList) {
                String batchName = batchConfig.get(DMConfigurationConstants.BATCH_NAME);
                int refLevel = Integer.valueOf(batchConfig.get(DMConfigurationConstants.BATCH_REF_LEVEL));
//                String switchMode = batchNameToSwitchModeMap.get(batchName);
                if (batchName.equalsIgnoreCase(paramBatchName)) {

                    TwitterStream twitter = pullStreamConnection();
                    if (twitter != null) {
                        try {
                            LinkedList<String> userScreenNames = new LinkedList<String>();
                            if (batchNameToInquiredUserListMap.get(batchName) != null) {
                                userScreenNames.addAll(batchNameToInquiredUserListMap.get(batchName));
                            }
                            if (batchNameToRegisteredUserListMap.get(batchName) != null) {
                                userScreenNames.addAll(batchNameToRegisteredUserListMap.get(batchName));
                            }
                            long[] userIds = new long[userScreenNames.size()];
                            for (int index = 0; index < userScreenNames.size() && index < 200; index++) {
                                String user = userScreenNames.get(index);
                                Long userId = twitterRegisteredUserScreenNameToIdMap.get(user);
                                if (userId != null) {
                                    userIds[index] = userId;
                                }
                                index++;
                            }
                            if (userIds.length > 0) {
                                FilterQuery filter = new FilterQuery(userIds);
                                String runCutoffMaxDate = batchConfig.get(DMConfigurationConstants.BATCH_CUTOFF_MAX_DATE);
                                String dateFormat = batchConfig.get(DMConfigurationConstants.BATCH_DATE_FORMAT);
                                long batchCutOffMaxDateLong;
                                if (runCutoffMaxDate == null || runCutoffMaxDate.length() == 0) {
                                    batchCutOffMaxDateLong = Long.MAX_VALUE;
                                } else {
                                    batchCutOffMaxDateLong = new SimpleDateFormat(dateFormat).parse(runCutoffMaxDate).getTime();
                                }
                                DMTargetWritter.log("Scheduling Twitter stream connection for batch [" + batchName + "] for Forward Pagination mode.");
                                twitter.cleanUp();
                                twitter.clearListeners();
                                twitter.addListener(new DMStatusListner(batchName, refLevel, batchCutOffMaxDateLong, twitter));
                                twitter.filter(filter);
//                                batchNameToSwitchModeMap.put(batchName, "false");

//                                batchConfigListBuffer.remove(batchConfig);
                            }
                        } catch (java.text.ParseException ex) {
                            DMTargetWritter.log(ex);
                        }
                    } else {
                        DMTargetWritter.log(new Exception("Twitter stream connection is null while scheduling streaming job"));

                        /**
                         * Logic to handle more batch than twitter connection
                         * API
                         *
                         */
                    }
                }
            }
//            DMConfigurationLoader.batchConfigList.clear();
//            DMConfigurationLoader.batchConfigList.addAll(batchConfigListBuffer);
        } catch (Exception ex) {
            DMTargetWritter.log(ex);
            if (e.getMessage().equalsIgnoreCase(ex.getMessage())) {
                DMTargetWritter.log(new Exception("Stuck in Exception loop"));
            } else {
                scheduleStreamToFollow(ex, paramBatchName);
            }
        }
    }

    private static synchronized void scheduleStreamToTrack(Exception e, String paramBatchName) {
//        List<LinkedHashMap<String, String>> batchConfigListBuffer = new ArrayList<LinkedHashMap<String, String>>();

//        batchConfigListBuffer.addAll(DMConfigurationLoader.batchConfigList);
        try {
            LinkedList<String> keywords = new LinkedList<String>();

            for (LinkedHashMap<String, String> batchConfig : DMConfigurationLoader.batchConfigList) {
                String batchName = batchConfig.get(DMConfigurationConstants.BATCH_NAME);
//                String switchMode = batchNameToSwitchModeMap.get(batchName);
//                String switchModeTrack = batchNameToSwitchModeMap.get(DMStatusListner.FILTER_TYPE_TRACKER);

                if (paramBatchName.equalsIgnoreCase(DMStatusListner.FILTER_TYPE_TRACKER) || !hasTrackerStarted) {

                    if ((batchConfig.get(DMConfigurationConstants.BATCH_TRACK_STREAM).equalsIgnoreCase("true"))) {

                        try {
                            LinkedList<String> userScreenNames = new LinkedList<String>();
                            if (batchNameToInquiredUserListMap.get(batchName) != null) {
                                userScreenNames.addAll(batchNameToInquiredUserListMap.get(batchName));
                            }
//                            if (batchNameToRegisteredUserListMap.get(batchName) != null) {
//                                userScreenNames.addAll(batchNameToRegisteredUserListMap.get(batchName));
//                            }
                            dbPatternReaderLock = true;
                            orgUserToPopularKeywordsMap.putAll(getPopularKeywordsMap(userScreenNames));
                            dbPatternReaderLock = false;
                            List<String> keywordsBuffer = getKeywords(orgUserToPopularKeywordsMap);

                            for (String keyword : keywordsBuffer) {
                                if (!keywords.contains(keyword)) {
                                    keywords.add(keyword);
                                }
                            };
                            if (keywords.size() == 400) {
                                setupStreamTracking(keywords);
                                keywords = null;
                            }
//                            if (userIds.length > 0) {
//                                FilterQuery filter = new FilterQuery();
//                                filter.track(userIds);
//                                twitter.cleanUp();
//                                twitter.clearListeners();
//                                twitter.addListener(new DMStatusListner(batchName, refLevel, batchCutOffMaxDateLong, twitter));
//                                twitter.filter(filter);
//                            batchNameToSwitchModeMap.put(batchName, "false");
                            String keywordString = "";
                            boolean first = true;
                            for (String keyword : keywords) {
                                if (!first) {
                                    keywordString += ",";
                                }
                                keywordString += keyword;
                                first = false;
                            }
                            if (keywordString.isEmpty()) {
                                DMTargetWritter.log("no keywords to be tracked for  batch [" + batchName + "]");
                            } else {
                                DMTargetWritter.log("Twitter stream connection will be used for batch [" + batchName + "]  to track the following keywords:" + keywordString);
                            }

//                                batchConfigListBuffer.remove(batchConfig);
//                            }
                        } catch (Exception ex) {
                            DMTargetWritter.log(ex);
                            if (e.getMessage().equalsIgnoreCase(ex.getMessage())) {
                                DMTargetWritter.log(new Exception("Stuck in Exception loop"));
                            } else {
                                scheduleStreamToTrack(ex, paramBatchName);
                            }

                        }

                    }
                }

            }
            if (keywords != null && keywords.size() > 0) {
                setupStreamTracking(keywords);
            }
            batchNameToSwitchModeMap.put(DMStatusListner.FILTER_TYPE_TRACKER, "false");

//            DMConfigurationLoader.batchConfigList.clear();
//            DMConfigurationLoader.batchConfigList.addAll(batchConfigListBuffer);
        } catch (Exception ex) {
            DMTargetWritter.log(ex);
            if (e.getMessage().equalsIgnoreCase(ex.getMessage())) {
                DMTargetWritter.log(new Exception("Stuck in Exception loop"));
            } else {
                scheduleStreamToTrack(ex, paramBatchName);
            }
        }
    }

    private static synchronized void setupStreamTracking(LinkedList<String> keywords) {
        if (keywords.size() > 0) {

            TwitterStream twitter = pullStreamConnection();
            if (twitter != null) {

                FilterQuery filter = new FilterQuery();
                String[] keywordArray = new String[keywords.size()];
                int i = 0;
                for (String keyword : keywords) {
                    keywordArray[i] = keyword;
                    i++;
                }
                filter.track(keywordArray);
                twitter.cleanUp();
                twitter.clearListeners();
//                list.add("Slate 6");

                twitter.addListener(new DMStatusListner(twitter, orgUserToPopularKeywordsMap));
                twitter.filter(filter);
                hasTrackerStarted = true;

//                                batchConfigListBuffer.remove(batchConfig);
            } else {
                DMTargetWritter.log(new Exception("Twitter stream connection is not available while scheduling streaming job in Traking mode"));

                /**
                 * Logic to handle more batch than twitter connection API
                 *
                 */
            }
        }
    }

    public static void main(String args[]) {

        try {
            String appConfigFileName = "";
            String logFileDir = "";

            if (args.length == 0) {
                System.out.println("Invalid  option!\nUsasge: java -jar <JAR File Name> <Options> \n\t\t-[Default]  \n\t\t-[Auth(optional)] <Authentication File Name> \n\t\t-[Log(Optional)] <Log File directory>");

                System.exit(0);
            } else {
                int index = 0;
                String key = args[index];
                boolean def = false;
                if (key.equalsIgnoreCase("-Default")) {
                    def = true;
                    index++;
                }
                if (!def && key.equalsIgnoreCase("-Auth")) {
                    appConfigFileName = args[index + 1];
                    index++;
                }
                if (!def && key.equalsIgnoreCase("-Log")) {
                    logFileDir = args[index + 1];
                }

            }

            DMTargetWritter.logFileDir = logFileDir;
            File file = new File(DMTargetWritter.logFileDir);
            if (!file.isDirectory()) {
                file = new File("log/");
                file.mkdirs();
                DMTargetWritter.logFileDir = "log/";

            }

            DMController control = new DMController(appConfigFileName);
            control.scheduleBatch();
        } catch (IOException ex) {
            DMTargetWritter.log(ex);
        } catch (ParseException ex) {
            DMTargetWritter.log(ex);
        }

    }
}
