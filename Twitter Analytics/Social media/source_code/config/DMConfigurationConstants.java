/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package config;

/**
 *
 * @author baradp
 */
public class DMConfigurationConstants {

    public static final String BATCH_TRACK_STREAM = "batchTrackStream";
    public static final String BATCH_REF_LEVEL = "batchRefLevel";
    public static final String BATCH_NAME = "batchName";
    public static final String BATCH_DATA_LOAD_REFRESH_RATE = "dataLoadRefreshRateMinute";

    public static final String BATCH_CUTOFF_MIN_DATE = "batchCutOffMinDate";
    public static final String BATCH_CUTOFF_MAX_DATE = "batchCutOffMaxDate";
    public static final String BATCH_DATE_FORMAT = "dateFormat";

    public static final String BATCH_RUN_START_TIME = "batchRunStartTime";

    public static final String BATCH_LINK_REPLY_USER = "linkReplyUser";
    public static final String BATCH_LINK_REPLY_LEVEL = "linkReplyLevel";
    public static final String BATCH_USER_NAMES = "userNames";
    //The Following three propoerties need to move to separate configuration file.
    public static final String APP_DB_SYNC_INTERVAL = "dbsyncIntervalMin";
    public static final String APP_CUST_COMMANDL_EXEC_INTERVAL = "customCommandIntervalMin";
    public static final String APP_ALLOW_PARALLEL_DB_LOADING = "allowParallelDBLoading";

    public static final String APP_FIELD_SEPARATOR = "fieldSeparator";

    public static final String APP_EMAIL_CONFIGURATION = "emailConfiguration";
    public static final String APP_EMAIL_SMTP_HOST = "smtpHost";
    public static final String APP_EMAIL_SMTP_PORT = "smtpPort";
    public static final String APP_EMAIL_SMTP_USER_NAME = "smtpUsername";
    public static final String APP_EMAIL_SMTP_PASSWORD = "smtpPassword";
    public static final String APP_EMAIL_SMTP_NATIVE_USER_NAME = "smtpNativeUserName";
    public static final String APP_EMAIL_PATTERN_NOTIFICATION = "patternNotification";
    public static final String APP_EMAIL_SUBJECT = "subject";
    public static final String APP_EMAIL_RECIPIENT = "recipients";
    public static final String APP_EMAIL_BODY = "body";

    public static final String APP_LINE_SEPARATOR = "lineSeparator";
    public static final String APP_SHUTDOWN_TIME = "shutdownTimeMinute";
    public static final String APP_CUSTOM_COMMAND = "customCommand";

    public static final String APP_WRITTER_BUFFER = "writerBufferKB";
    public static final String APP_DB_HOST = "dbHost";
    public static final String APP_DB_PORT = "dbPort";
    public static final String APP_DB_USERNAME = "dbUsername";
    public static final String APP_DB_PASSWORD = "dbPassword";
    public static final String APP_DB_NAME = "dbName";
    public static final String APP_TWITTER_USER_TABLE = "twitterUserTable";
    public static final String APP_TWITTER_STATUS_TABLE = "twitterStatusTable";

    public static final String APP_PROXY_HOST = "proxyHost";
    public static final String APP_PROXY_PORT = "proxyPort";
    public static final String APP_PROXY_USER = "proxyUser";
    public static final String APP_PROXY_PASSWORD = "proxyPassword";

    //Authentication config identifiers
    public static final String APP_CONNECTION_POOL = "connectionPool";
    public static final String APP_CONSUMER_KEY = "consumerKey";
    public static final String APP_CONSUMER_KEY_SECRET = "consumerKeySecret";
    public static final String APP_ACCESS_TOKEN = "accessToken";
    public static final String APP_ACCESS_TOKEN_SECRET = "accessTokenSecret";
    public static final String APP_BATCH_FILE_LIST = "batchFileList";
    public static final String APP_OUTPUT_DIR = "outputDirectory";
    public static final String APP_DIR_SEPARATOR = "directorySeparator";
}
