/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package writer;

import config.DMConfigurationConstants;
import config.DMConfigurationLoader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import loader.DMVerticaConnector;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author baradp
 */
public class DMTargetWritter {

    private static int writerBufferKB = 256;
    private static String fieldSeparator;
    private static String lineSeparator;
    public static String logFileDir;
    public static String serialDir;

    private static List<String> fileLoadQueue = new LinkedList<String>();
    private static List<String> fileLoadQueueReject = new LinkedList<String>();
    public static long nextDBSyncRefreshTimeLong;
    public static long dbSyncRefreshIntervalTimeLong;
    public static long nextCustCommandExecTimeLong;
    public static long customCommandExecIntervalTimeLong;
    public static boolean writeLock;

    public static String refreshTimeDateString;

    public static void init(int writerBufferKB, String fieldSeparator, String lineSeparator, long nextDBSyncRefreshIntervalMin, long nextCustSQLExceIntervalMin) {
        DMTargetWritter.writerBufferKB = writerBufferKB;
        DMTargetWritter.fieldSeparator = fieldSeparator;
        DMTargetWritter.lineSeparator = lineSeparator;
        DMTargetWritter.dbSyncRefreshIntervalTimeLong = nextDBSyncRefreshIntervalMin * 60 * 1000;
        DMTargetWritter.customCommandExecIntervalTimeLong = nextCustSQLExceIntervalMin * 60 * 1000;

        DMTargetWritter.nextDBSyncRefreshTimeLong = System.currentTimeMillis() + DMTargetWritter.dbSyncRefreshIntervalTimeLong;
        DMTargetWritter.nextCustCommandExecTimeLong = System.currentTimeMillis() + DMTargetWritter.customCommandExecIntervalTimeLong;

        DMTargetWritter.refreshTimeDateString = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(DMTargetWritter.nextDBSyncRefreshTimeLong));
        DMTargetWritter.log("Application started at:" + new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss").format(new Date(System.currentTimeMillis())));
        DMTargetWritter.log("This run will shutdown at:" + new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss").format(new Date(System.currentTimeMillis() + DMConfigurationLoader.applicationLifeTimeLong)));
        DMTargetWritter.log("Next Database Synchronization Time is:" + new SimpleDateFormat("dd/MM/yyy-HH:mm:ss").format(new Date(DMTargetWritter.nextDBSyncRefreshTimeLong)));
        DMTargetWritter.log("Next Custom SQL Execution Time is:" + new SimpleDateFormat("dd/MM/yyy-HH:mm:ss").format(new Date(DMTargetWritter.nextCustCommandExecTimeLong)));
    }

    public static int log(String information) {

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFileDir + "DataMiner_" + getTimestampString() + ".log", true), writerBufferKB * 1024);
            String content = lineSeparator + "[DEBUG:" + new Date().toString();
            content += "]" + information;
            writer.write(content);
            writer.close();
//            System.out.println(content);

        } catch (Exception ex) {
            return -1;
        }
        return 0;
    }

    public static int log(Exception e) {

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFileDir + "DataMiner_" + getTimestampString() + ".log", true), DMTargetWritter.writerBufferKB * 1024);
            String content = lineSeparator + "[DEBUG:" + new Date().toString();
            content += "] Unknown Exception occured:" + e.toString() + lineSeparator;
            content += "*******" + lineSeparator + Arrays.toString(e.getStackTrace()) + lineSeparator + "*******";

            writer.write(content);

            writer.close();
//            System.out.println(content);

        } catch (Exception ex) {
            return -1;
        }
        return 0;
    }

    public static int log(Error e) {

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(logFileDir + "DataMiner_" + getTimestampString() + ".log", true), DMTargetWritter.writerBufferKB * 1024);
            String content = lineSeparator + "[DEBUG:" + new Date().toString();
            content += "] Unknown Exception occured:" + e.toString() + lineSeparator;
            content += "*******" + lineSeparator + Arrays.toString(e.getStackTrace()) + lineSeparator + "*******";

            writer.write(content);

            writer.close();
//            System.out.println(content);

        } catch (Exception ex) {
            return -1;
        }
        return 0;
    }

    public synchronized static void syncToDB() {
        LinkedList<String> listBuffer = new LinkedList<String>();
        listBuffer.addAll(fileLoadQueue);
        for (String fileName : listBuffer) {
            if (fileName != null && !fileName.isEmpty()) {

                if (fileName.contains("STATUS")) {

                    boolean status = DMVerticaConnector.runCopyCommand(DMConfigurationLoader.dbHost, DMConfigurationLoader.dbPort,
                            DMConfigurationLoader.dbName, DMConfigurationLoader.dbUsername, DMConfigurationLoader.dbPassword,
                            fieldSeparator, lineSeparator, fileName, DMConfigurationLoader.twitterStatusTable);
                    if (status) {
                        fileLoadQueue.remove(fileName);
                        log("File removed from the queue:" + fileName);

                    } else {
                        DMTargetWritter.log("FATAL ERROR: Could not load File:" + fileName + "  to the Database");
                    }

                } else if (fileName.contains("USER")) {

                    boolean status = DMVerticaConnector.runCopyCommand(DMConfigurationLoader.dbHost, DMConfigurationLoader.dbPort,
                            DMConfigurationLoader.dbName, DMConfigurationLoader.dbUsername, DMConfigurationLoader.dbPassword,
                            fieldSeparator, lineSeparator, fileName, DMConfigurationLoader.twitterUserTable);
                    if (status) {
                        fileLoadQueue.remove(fileName);
                        log("File removed from the queue:" + fileName);

                    } else {
                        DMTargetWritter.log("FATAL ERROR: Could not load File:" + fileName + "  to the Database");
                    }
                } else {
                    log(new Exception("Could not map the output file to target twitter table"));

                }
            }
        }
        log("File Load Queue:" + fileLoadQueue.toString());
        updateBatchFiles();
        updatedDBSyncRefreshTime();
        fileLoadQueueReject.clear();
    }

    public synchronized static void updateBatchFiles() {

//        DMConfigurationLoader.batchConfigList.clear();
        for (String batchFile : DMConfigurationLoader.batchFileList) {
            try {
                JSONObject batchObj = (JSONObject) new JSONParser().parse(new FileReader(new File(batchFile).getCanonicalPath()));
                String dateFormat = (String)batchObj.get(DMConfigurationConstants.BATCH_DATE_FORMAT);
                Date date = new Date(System.currentTimeMillis());
                SimpleDateFormat dateFormatter = new SimpleDateFormat(dateFormat);
                batchObj.put(DMConfigurationConstants.BATCH_CUTOFF_MIN_DATE, dateFormatter.format(date));
                
//                DMConfigurationLoader.batchConfigList.add(batchMap);
                BufferedWriter writer = new BufferedWriter(new FileWriter(batchFile));
                ObjectMapper mapper = new ObjectMapper();
                String jsonString = mapper.defaultPrettyPrintingWriter().writeValueAsString(batchObj);
                writer.write(jsonString);
                writer.close();
            } catch (IOException ex) {
                Logger.getLogger(DMTargetWritter.class.getName()).log(Level.SEVERE, null, ex);
            } catch (ParseException ex) {
                Logger.getLogger(DMTargetWritter.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    public synchronized static void execCustomCommand() {
        new Thread() {
            @Override

            public void run() {
                if (!DMConfigurationLoader.allowParallelDBLoading) {
                    writeLock = true;
                }
                DMVerticaConnector.runCustomCommand(DMConfigurationLoader.customCommand);
                updatedCustSQLExecTime();
                writeLock = false;

            }
        }.start();
    }

    public static void writeRecordList(LinkedList<LinkedHashMap<String, String>> rowList, String outputFileName) {
        for (LinkedHashMap row : rowList) {
            write(row, outputFileName);
        }
    }

    private static void write(Map<String, String> row, String outputFileName) {
        boolean first = true;
        String header = "";

        /**
         * If it is a new file then write Header row
         *
         *
         */
        /**
         * Add the file to the load queue if it has already been not loaded
         */
        if (outputFileName.toUpperCase().contains("REJECTED")) {
            if (!fileLoadQueueReject.contains(outputFileName)) {
                fileLoadQueueReject.add(outputFileName);

                log("File will not be loaded to Database:" + outputFileName);
            }
        } else {
            if (!fileLoadQueue.contains(outputFileName)) {
                fileLoadQueue.add(outputFileName);
                log("File added to the queue:" + outputFileName);

            }
        }
        File outputFile = new File(outputFileName);
        if (!outputFile.exists()) {
            for (Map.Entry entry : row.entrySet()) {
                if (!first) {
                    header += fieldSeparator;
                }
                header += (String) entry.getKey();
                first = false;

            }
            header += lineSeparator;
        }
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(outputFileName, true), DMTargetWritter.writerBufferKB * 1024);
        } catch (IOException ex) {
            log(ex);

        }
        /**
         * Write the data row
         */
        /**
         * Reset the first-field-flag
         */
        first = true;
        String content = "";
        for (Map.Entry entry : row.entrySet()) {
            if (!first) {
                content += fieldSeparator;
            }
            String field = (String) entry.getValue();
            if (field != null) {
                field = field.replace("\\", "\\\\").replace("\"", "\\\"").replace("null", "");
            }
            if (field != null && field.length() > 0) {
                field = "\"" + field + "\"";
            }
            content += field;

            first = false;

        }

        content = content.replace("\n", " ").replace("\r", " ");
        header = header.replace("\n", " ").replace("\r", " ");

        if (!header.isEmpty()) {
            content = header + lineSeparator + content + lineSeparator;
        } else {
            content += lineSeparator;
        }

        try {
            //        writer.write(lineSeparator);
            writer.write(content);
            writer.close();

//        numberOfRecords = numberOfRecords + (content.getBytes().length / 1000);
//        System.out.println(outputFileName);
        } catch (IOException ex) {
            log(ex);
        }
//        loadToDB(outputFileName);
//        String fileNameDateString = outputFileName.substring(outputFileName.length() - 8);
//        int fileTime = Integer.valueOf(fileNameDateString);
//        int dbTime = Integer.valueOf(DMTargetWritter.refreshTimeDateString);
//        if (fileTime < dbTime) {
//            syncToDB();
//        }
        if (!writeLock && System.currentTimeMillis() >= DMTargetWritter.nextDBSyncRefreshTimeLong) {
            syncToDB();
        }
        if (!writeLock && System.currentTimeMillis() >= DMTargetWritter.nextCustCommandExecTimeLong) {
            execCustomCommand();
        }
    }

//    private static void loadToDB(String outputFileName) {
//        String userName = outputFileName.substring(outputFileName.indexOf(DMConfigurationLoader.directorySeparator) + 2, outputFileName.indexOf("-"));
//        try {
//            outputFileName = new File(outputFileName).getCanonicalPath();
//        } catch (IOException ex) {
//            log(ex);
//        }
//
//        //If new file is first file on the stack then fileToBeLoaded will be null and therefore no files will be loaded to vertica
//        if (outputFileName.contains("STATUS")) {
//
//            if (DMController.userNameToStatusFilesMap.get(userName) == null) {
//                DMController.userNameToStatusFilesMap.put(userName, new LinkedList<String>());
//                DMController.userNameToStatusFilesMap.get(userName).add(outputFileName);
//
//            } else if (!DMController.userNameToStatusFilesMap.get(userName).contains((String) outputFileName)) {
//                //When new file is created the latest file from the stack is loaded to Vertica using copy command
//                String fileToBeLoaded = "";
//
//                fileToBeLoaded = DMController.userNameToStatusFilesMap.get(userName).getLast();
//
//                if (fileToBeLoaded != null && !fileToBeLoaded.isEmpty()) {
//                    DMVerticaConnector.runCopyCommand(DMConfigurationLoader.dbHost, DMConfigurationLoader.dbPort, DMConfigurationLoader.dbName, DMConfigurationLoader.dbUsername, DMConfigurationLoader.dbPassword,
//                            fieldSeparator, lineSeparator, fileToBeLoaded, DMConfigurationLoader.twitterStatusTable);
//                    DMController.userNameToStatusFilesMap.get(userName).add(outputFileName);
//
//                }
//            }
//
//        } else if (outputFileName.contains("USER")) {
//
//            if (DMController.userNameToUserFilesMap.get(userName) == null) {
//                DMController.userNameToUserFilesMap.put(userName, new LinkedList<String>());
//                DMController.userNameToUserFilesMap.get(userName).add(outputFileName);
//
//            } else if (!DMController.userNameToUserFilesMap.get(userName).contains((String) outputFileName)) {
//
//                //When new file is created the latest file from the stack is loaded to Vertica using copy command
//                String fileToBeLoaded = "";
//
//                fileToBeLoaded = DMController.userNameToUserFilesMap.get(userName).getLast();
//
//                if (fileToBeLoaded != null && !fileToBeLoaded.isEmpty()) {
//                    DMVerticaConnector.runCopyCommand(DMConfigurationLoader.dbHost, DMConfigurationLoader.dbPort, DMConfigurationLoader.dbName, DMConfigurationLoader.dbUsername, DMConfigurationLoader.dbPassword,
//                            fieldSeparator, lineSeparator, fileToBeLoaded, DMConfigurationLoader.twitterUserTable);
//                    DMController.userNameToUserFilesMap.get(userName).add(outputFileName);
//
//                }
//            }
//
//        } else {
//            log(new Exception("Could not map the output file to target twitter table"));
//
//        }
//    }
    public static String getTimestampString() {
        String timestamp = new SimpleDateFormat("yyyyMMddHH").format(new Date());
        return timestamp;
    }

    public static void updatedDBSyncRefreshTime() {
        if (System.currentTimeMillis() >= nextDBSyncRefreshTimeLong) {
            nextDBSyncRefreshTimeLong = System.currentTimeMillis() + dbSyncRefreshIntervalTimeLong;
            refreshTimeDateString = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(nextDBSyncRefreshTimeLong));
            DMTargetWritter.log("Next Database Synchronization Time is:" + new SimpleDateFormat("dd/MM/yyy-HH:mm:ss").format(new Date(DMTargetWritter.nextDBSyncRefreshTimeLong)));
        }
    }

    public static void updatedCustSQLExecTime() {
        if (System.currentTimeMillis() >= nextCustCommandExecTimeLong) {
            nextCustCommandExecTimeLong = System.currentTimeMillis() + customCommandExecIntervalTimeLong;
            DMTargetWritter.log("Next Custom SQL Execution Time is:" + new SimpleDateFormat("dd/MM/yyy-HH:mm:ss").format(new Date(DMTargetWritter.nextCustCommandExecTimeLong)));
        }
    }

}
