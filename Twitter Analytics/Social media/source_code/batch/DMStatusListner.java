/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch;

import config.DMConfigurationLoader;
import engine.DMController;
import static engine.DMController.batchNameToInquiredUserListMap;
import static engine.DMController.twitterRegisteredUserScreenNameToIdMap;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import twitter4j.DirectMessage;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.SymbolEntity;
import twitter4j.TwitterStream;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserList;
import twitter4j.UserMentionEntity;
import twitter4j.UserStreamListener;
import writer.DMTargetWritter;

/**
 *
 * @author baradp
 */
public class DMStatusListner implements UserStreamListener {

//    public static final String STREAM_OUTPUT_FILE_KEYWORD = "STREAM";
    public static final String FILTER_TYPE_TIMELINE = "STREAM_TIMELINE";
    public static final String FILTER_TYPE_TRACKER = "STREAM_TRACKER";
    public static final String ORIGINAL_USER_TYPE_KEYWORD = "STREAM_KEYWORD";

    public static final String FILTER_TYPE_REFERENCE = "STREAM_REFERENCE";
    private static LinkedHashMap<String, LinkedList<String>> orgUserToKeywordsList = new LinkedHashMap<String, LinkedList<String>>();
    private String batchName;
    private int referenceLevel = 2;
    private int referenceLevelBuffer = 2;
    private long batchCutOffMaxDateLong;
    private static long nextSynchTimeLong = -1;
    private static long synchWaitTimeLong = 1 * 60 * 1000;
    private static boolean onStatusErrorHandled = false;

//    private TwitterStream streamInstance;
    private static HashMap<String, TwitterStream> batchNameToStreamInstanceMap = new HashMap<String, TwitterStream>();
    private static LinkedList<String> scheduledBatchList = new LinkedList<String>();

    public DMStatusListner(String batchName, int refLevel, long batchCutOffMaxDateLong, TwitterStream streamInstance) {
        updateLastSyncTime();
        this.batchName = batchName;
        this.referenceLevel = this.referenceLevelBuffer = refLevel;
        this.batchCutOffMaxDateLong = batchCutOffMaxDateLong;
        batchNameToStreamInstanceMap.put(batchName, streamInstance);
        scheduledBatchList.add(batchName);
        DMTargetWritter.log("Scheduled batch: " + batchName + " =" + batchNameToInquiredUserListMap.get(batchName).toString());

    }

    public DMStatusListner(TwitterStream streamInstance, LinkedHashMap<String, LinkedList<String>> orgUserToKeywordsList) {
        updateLastSyncTime();

        DMStatusListner.orgUserToKeywordsList.putAll(orgUserToKeywordsList);
        this.batchName = "STREAM_TRACKER";
//        this.referenceLevel = this.referenceLevelBuffer = refLevel;
        this.batchCutOffMaxDateLong = Long.MAX_VALUE;
        batchNameToStreamInstanceMap.put(batchName, streamInstance);
        scheduledBatchList.add(batchName);
        DMTargetWritter.log("Scheduled batch: " + batchName);

    }

    private static void updateLastSyncTime() {
        if (nextSynchTimeLong == -1) {
            nextSynchTimeLong = DMTargetWritter.nextDBSyncRefreshTimeLong + DMTargetWritter.dbSyncRefreshIntervalTimeLong;
        }
    }

    public synchronized static void resynchStreamFilters(Exception e) {
        if (System.currentTimeMillis() >= nextSynchTimeLong) {
            nextSynchTimeLong = nextSynchTimeLong + 2 * DMTargetWritter.dbSyncRefreshIntervalTimeLong;
            LinkedList<String> batchNameListBuffer = new LinkedList<String>();
            batchNameListBuffer.addAll(scheduledBatchList);
            DMTargetWritter.log("Resynchronizing all Stream Batch:" + scheduledBatchList.toString());
            shutdownStream();

            for (String localBatchName : batchNameListBuffer) {
                try {
                    Thread.sleep(synchWaitTimeLong);
                } catch (InterruptedException ex) {
                    if (e == null || (ex != null && !e.getMessage().equalsIgnoreCase(ex.getMessage()))) {
                        resynchStreamFilters(ex);
                    }
                }
                DMController.batchNameToSwitchModeMap.put(localBatchName, "true");

                DMController.scheduleStream(localBatchName);
                if (localBatchName.equals(FILTER_TYPE_TRACKER)) {
                    DMTargetWritter.log("Keyword list for each original user after resynch is:" + orgUserToKeywordsList.toString());
                } else {
                    String users = DMController.batchNameToInquiredUserListMap.get(localBatchName).toString();
                    if (users != null) {
                        DMTargetWritter.log("User list for batch [" + localBatchName + "] after resynch is:" + users);
                    }
                }
            }
            onStatusErrorHandled = false;
            try {
                DMController.serialize();
            } catch (IOException ex) {
                DMTargetWritter.log(ex);
            }

        }
    }

    private static void shutdownStream(String batchName) {
//        if (DMStatusListner.orgUserToKeywordsList.size() == 0) {
//            DMTargetWritter.log("[" + batchName + "] batch will no longer receive streaming data. ");
//        } else {
//            DMTargetWritter.log("[" + batchName + "] batch will no longer receive streaming data. ");
//
//        }
        DMTargetWritter.log("Stream Connections for batch [" + batchName + "] is closed because current status created exceeded Batch Cutoff date.");

        if (batchName.equalsIgnoreCase(FILTER_TYPE_TRACKER)) {
            orgUserToKeywordsList.clear();
        }
        scheduledBatchList.remove(batchName);
        DMController.releaseStreamConnection(batchNameToStreamInstanceMap.get(batchName));
    }

    private static synchronized void shutdownStream() {
//        if (DMStatusListner.orgUserToKeywordsList.size() == 0) {
//            DMTargetWritter.log("[" + batchName + "] batch will no longer receive streaming data. ");
//        } else {
//            DMTargetWritter.log("[" + batchName + "] batch will no longer receive streaming data. ");
//
//        }
        LinkedList<String> batchNameListBuffer = new LinkedList<String>();
        batchNameListBuffer.addAll(scheduledBatchList);

        for (String localBatchName : batchNameListBuffer) {

            DMTargetWritter.log("Stream Connections for batch [" + localBatchName + "] is closed for resynchronization.");

            if (localBatchName.equalsIgnoreCase(FILTER_TYPE_TRACKER)) {
                orgUserToKeywordsList.clear();
            }
            scheduledBatchList.remove(localBatchName);
            DMController.releaseStreamConnection(batchNameToStreamInstanceMap.get(localBatchName));
        }
    }

    @Override
    public void onStatus(Status status) {
        try {
            if (Runtime.getRuntime().freeMemory() / (1024 * 1024) > (Runtime.getRuntime().totalMemory() / (1024 * 1024)) * .10) {
                resynchStreamFilters(null);
                long currentStatusDateLong = System.currentTimeMillis();
                if (currentStatusDateLong < DMConfigurationLoader.shutdownTime) {

                    if (currentStatusDateLong < this.batchCutOffMaxDateLong) {
                        try {
                            if (!this.batchName.equalsIgnoreCase(FILTER_TYPE_TRACKER)) {
                                if (DMController.batchNameToInquiredUserListMap.get(this.batchName).contains(status.getUser().getScreenName())) {
                                    /**
                                     * Reinitialize the Reference level.
                                     */

                                    this.referenceLevel = this.referenceLevelBuffer;
                                    String orgUserFilter = getOriginalUser(status.getUser().getScreenName());

                                    this.getStatusDetails(status, FILTER_TYPE_TIMELINE, status.getUser().getScreenName(), -1, orgUserFilter);
                                } else {
                                    /**
                                     * Reset the reference Level if it is
                                     * already a Reference Status
                                     */

                                    this.referenceLevel = 0;

                                    this.getStatusDetails(status, FILTER_TYPE_REFERENCE, this.getReferenceNameForTweet(status.getUserMentionEntities()), -1, null);
                                }
                            } else {
                                this.referenceLevel = 0;

                                try {
                                    String[] pattern = this.getPattern(status);
                                    if (pattern[0] != null && !pattern[0].isEmpty()) {

                                        this.getStatusDetails(status, FILTER_TYPE_TRACKER, pattern[0], -1, pattern[1]);
                                    }
                                } catch (IOException ex) {
                                    DMTargetWritter.log(ex);
                                }

                            }
                        } catch (IOException ex) {
                            DMTargetWritter.log(ex);
                        }
                    } else {
                        DMTargetWritter.log("Maximum cutoff date achieved for batch:" + this.batchName);
                        shutdownStream(batchName);
                    }
                } else {

                    DMTargetWritter.log("INFO: Application will shutdown after Thread [" + Thread.currentThread().getId() + "] completes!!!");
                    DMController.serializeProcessedStatuses();
                    DMController.serializeProcessedUsers();
                    DMController.serialize();
                    shutdownStream();
                    System.exit(0);

                }

            } else {
                if (!onStatusErrorHandled) {
                    onStatusErrorHandled = true;
                    DMTargetWritter.log("Running out of Memory error while handling onStatus request, force resynchronization of all stream batch after serialization");
                    DMTargetWritter.log("------Before resync------");
                    DMTargetWritter.log("Total Heap size:" + Runtime.getRuntime().totalMemory() / (1024 * 1024) + " MB");

                    DMTargetWritter.log("Free Heap size:" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + " MB");
                    DMTargetWritter.log("Maximum Heap size:" + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB");
                    DMController.orgUserToPopularKeywordsMap.clear();

                    DMController.serializeProcessedStatuses();
                    DMController.serializeProcessedUsers();
                    nextSynchTimeLong = System.currentTimeMillis();

//                    System.gc();
                    resynchStreamFilters(null);

                    DMTargetWritter.log("------After resync------");
                    DMTargetWritter.log("Total Heap size:" + Runtime.getRuntime().totalMemory() / (1024 * 1024) + " MB");

                    DMTargetWritter.log("Free Heap size:" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + " MB");
                    DMTargetWritter.log("Maximum Heap size:" + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB");
                }
            }
        } catch (OutOfMemoryError e) {

            DMTargetWritter.log("Could not handle Out of Memory error while handling onStatus request, force resynchronization of all stream batch after deserialization");
            DMTargetWritter.log("Total Heap size:" + Runtime.getRuntime().totalMemory() / (1024 * 1024) + " MB");
            DMTargetWritter.log("Free Heap size:" + Runtime.getRuntime().freeMemory() / (1024 * 1024) + " MB");
            DMTargetWritter.log("Maximum Heap size:" + Runtime.getRuntime().maxMemory() / (1024 * 1024) + " MB");
            DMController.orgUserToPopularKeywordsMap.clear();

            try {
                DMController.deserialize();

            } catch (IOException ex) {
                DMTargetWritter.log(ex);
            } catch (ClassNotFoundException ex) {
                DMTargetWritter.log(ex);
            }
            DMTargetWritter.log("After deserialization twitterRegisteredUserScreenNameToIdMap size:" + twitterRegisteredUserScreenNameToIdMap.size());
            DMTargetWritter.log("After deserialization batchNameToRegisteredUserListMap size:" + DMController.batchNameToRegisteredUserListMap.size());
            DMTargetWritter.log("After deserialization derToOrgUserNameMap size:" + DMController.derToOrgUserNameMap.size());
            nextSynchTimeLong = System.currentTimeMillis();
            resynchStreamFilters(null);

        } catch (Exception e) {
            if (!onStatusErrorHandled) {
                onStatusErrorHandled = true;

                DMTargetWritter.log(e);
                DMTargetWritter.log("Exception while handling onStatus request, force resynchronization of all stream batch after serialization");
                nextSynchTimeLong = System.currentTimeMillis();
                resynchStreamFilters(null);
            }
        }

    }

    private String[] getPattern(Status status) {

        String[] pattern = new String[2];
        String text = status.getText();
        boolean firstOrgUser = true;
        boolean firstPattern = true;
        List<String> identifiedPatterns = new ArrayList<String>();
        String orgUser = "";
        for (Map.Entry<String, LinkedList<String>> entry : orgUserToKeywordsList.entrySet()) {
            LinkedList<String> keywords = entry.getValue();
            boolean matchBoth = false;

            for (String keyword : keywords) {
                StringTokenizer tokenizer = new StringTokenizer(keyword, "+");
                String derUser = "";
                String hashTag = "";
                if (tokenizer.hasMoreTokens()) {
                    derUser = tokenizer.nextToken();
                }
                if (tokenizer.hasMoreTokens()) {
                    hashTag = tokenizer.nextToken();
                }
                tokenizer = null;
                boolean matchHashtag = false;
                boolean matchDerUser = false;

                if (text.toLowerCase().contains(hashTag.toLowerCase())) {
                    matchHashtag = true;
                }
                if (text.toLowerCase().contains(derUser.toLowerCase())) {
                    matchDerUser = true;
                }
                if (matchHashtag && matchDerUser) {
                    if (!firstPattern) {
                        pattern[0] += " & ";
                    }
                    pattern[0] += "(" + keyword + ")";
                    firstPattern = false;
                    matchBoth = true;
                } else if (matchHashtag && !identifiedPatterns.contains(hashTag)) {
                    if (!firstPattern) {
                        pattern[0] += " & ";
                    }
                    pattern[0] += "(" + hashTag + ")";
                    firstPattern = false;
                    identifiedPatterns.add(hashTag);

                } else if (matchDerUser && !identifiedPatterns.contains(derUser)) {
                    if (!firstPattern) {
                        pattern[0] += " & ";
                    }
                    pattern[0] += "(" + derUser + ")";
                    firstPattern = false;
                    identifiedPatterns.add(derUser);

                }
            }
            if (matchBoth) {
                if (!firstOrgUser) {
                    orgUser += " & ";
                }
                orgUser += entry.getKey();
                firstOrgUser = false;

            }
        }
        if (!orgUser.isEmpty()) {

            pattern[1] = orgUser;
        } else {
            pattern[1] = ORIGINAL_USER_TYPE_KEYWORD;
        }
        return pattern;
    }

    private String getReferenceNameForTweet(UserMentionEntity[] userMentionEntitys) {
        String refNames = "";
        boolean first = true;
        for (UserMentionEntity userMention : userMentionEntitys) {
            if (!first) {
                refNames += "+";
            }
            refNames += userMention.getScreenName();
            first = false;

        }
        return refNames;
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn) {

    }

    @Override
    public void onTrackLimitationNotice(int i) {

    }

    @Override
    public void onScrubGeo(long l, long l1) {
    }

    @Override
    public void onStallWarning(StallWarning sw) {
        try {
            DMTargetWritter.log("Stall Warning: Input queue is " + sw.getPercentFull() + "% full.");
        } catch (Exception e) {
            DMTargetWritter.log(e);
        }

    }

    @Override
    public void onException(Exception excptn) {

    }

    private String getOriginalUser(String derivedUser) {
        String orgUser = "";
        orgUser = DMController.derToOrgUserNameMap.get(derivedUser);

        return orgUser;
    }

    private void getUserDetails(User user, long tweetId, String filterType, String derivedUserFilter, String orgUserFilter) throws IOException {
        LinkedHashMap<String, String> currRowMap = new LinkedHashMap<String, String>();
        currRowMap.put("userFilterType", filterType);

        currRowMap.put("userOriginalUserFilter", orgUserFilter);

        currRowMap.put("userDerivedUserFilter", derivedUserFilter);

        currRowMap.put("systemCreateDt", new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss").format(new Date(System.currentTimeMillis())));
//        currRowMap.put("userRefStatusId", String.valueOf(tweetId));
        currRowMap.put("userName", user.getName());
        currRowMap.put("userId", Long.toString(user.getId()));

        currRowMap.put("userLocation", user.getLocation());

        if (user.getCreatedAt() == null) {
            currRowMap.put("userCreatedAt", null);
        } else {
            currRowMap.put("userCreatedAt", user.getCreatedAt().toString());
        }
        currRowMap.put("userLang", user.getLang());
        currRowMap.put("userScreenName", user.getScreenName());
        currRowMap.put("userTimeZone", user.getTimeZone());
        currRowMap.put("userURL", user.getURL());

        currRowMap.put("userFavouritesCount", Integer.toString(user.getFavouritesCount()));
        currRowMap.put("userFollowersCount", Integer.toString(user.getFollowersCount()));
        currRowMap.put("userFriendsCount", Integer.toString(user.getFriendsCount()));
        currRowMap.put("userFriendsCount", Integer.toString(user.getListedCount()));
        currRowMap.put("userListedCount", Integer.toString(user.getListedCount()));

        currRowMap.put("userStatusesCount", Integer.toString(user.getStatusesCount()));
        currRowMap.put("userUtcOffset", Integer.toString(user.getUtcOffset()));
        currRowMap.put("userProfileBannerIPadURL", user.getProfileBannerIPadURL());

        /**
         * Map User Profile graphic details
         */
        currRowMap.put("userBiggerProfileImageURL", user.getBiggerProfileImageURL());
        currRowMap.put("userBiggerProfileImageURLHttps", user.getBiggerProfileImageURLHttps());
        currRowMap.put("userDescription", user.getDescription());
        currRowMap.put("userMiniProfileImageURL", user.getMiniProfileImageURL());
        currRowMap.put("userMiniProfileImageURLHttps", user.getMiniProfileImageURLHttps());
        currRowMap.put("userOriginalProfileImageURL", user.getOriginalProfileImageURL());
        currRowMap.put("userOriginalProfileImageURLHttps", user.getOriginalProfileImageURLHttps());
        currRowMap.put("userProfileBackgroundColor", user.getProfileBackgroundColor());
        currRowMap.put("userProfileBackgroundImageURL", user.getProfileBackgroundImageURL());
        currRowMap.put("userProfileBackgroundImageUrlHttps", user.getProfileBackgroundImageUrlHttps());
        currRowMap.put("userProfileBannerIPadRetinaURL", user.getProfileBannerIPadRetinaURL());
        currRowMap.put("userProfileBannerMobileRetinaURL", user.getProfileBannerMobileRetinaURL());
        currRowMap.put("userProfileBannerMobileURL", user.getProfileBannerMobileURL());
        currRowMap.put("userProfileBannerRetinaURL", user.getProfileBannerRetinaURL());
        currRowMap.put("userProfileBannerURL", user.getProfileBannerURL());
        currRowMap.put("userProfileImageURL", user.getProfileImageURL());
        currRowMap.put("userProfileImageURLHttps", user.getProfileImageURLHttps());
        currRowMap.put("userProfileLinkColor", user.getProfileLinkColor());
        currRowMap.put("userProfileSidebarBorderColor", user.getProfileSidebarBorderColor());
        currRowMap.put("userProfileSidebarFillColor", user.getProfileSidebarFillColor());
        currRowMap.put("userProfileTextColor", user.getProfileTextColor());
        currRowMap.put("userURLEntity", user.getURLEntity().getText());

        if (user.getDescriptionURLEntities() != null) {
            int i = 0;
            for (URLEntity entity : user.getDescriptionURLEntities()) {
                currRowMap.put("userDescriptionURLEntities_" + i, entity.getURL());
                i++;

            }
            getNullValueMap(currRowMap, "userDescriptionURLEntities_", i);
        } else {
            getNullValueMap(currRowMap, "userDescriptionURLEntities_", 0);
        }
        /**
         * Write extracted record to target
         */
        LinkedList<LinkedHashMap<String, String>> rowList = new LinkedList<LinkedHashMap<String, String>>();
        rowList.add(currRowMap);
        if (batchName.equals(FILTER_TYPE_TRACKER) && (orgUserFilter == null || orgUserFilter.isEmpty()
                || orgUserFilter.equals(ORIGINAL_USER_TYPE_KEYWORD))) {
            this.writeRecord(rowList, "REJECTED-USER");

        } else if (!DMController.lookupCacheFileUser(user.getScreenName())) {
            this.writeRecord(rowList, "USER");
        }

        /**
         * Clear the map after writing to target
         *
         */
        currRowMap.clear();
        rowList.clear();

    }

//    private void writeRecord(Map<String, String> currRowMap, String keyword) throws IOException {
//        String outputFileName = DMConfigurationLoader.outputDirectory + "[" + batchName + "-" + keyword + "]";
//        String dateString = DMTargetWritter.refreshTimeDateString;
//        if (dateString != null) {
//            outputFileName += dateString;
//        }
//        DMTargetWritter.write(currRowMap, outputFileName);
//
//    }
    private void writeRecord(LinkedList<LinkedHashMap<String, String>> currRowMapList, String keyword) throws IOException {
        String outputFileName = DMConfigurationLoader.outputDirectory + "[" + batchName + "-" + keyword + "]";
        String dateString = DMTargetWritter.refreshTimeDateString;
        if (dateString != null) {
            outputFileName += dateString;
        }
        DMTargetWritter.writeRecordList(currRowMapList, outputFileName);
        currRowMapList.clear();

    }

    private void getNullValueMap(LinkedHashMap<String, String> map, String keyword, int startIndex) {
        for (int index = startIndex; index < 12; index++) {
            map.put(keyword + index, null);
        }
    }

    private void getStatusDetails(Status status, String filterType, String derivedUserFilter, long refStatusId, String orgUserFilter) throws IOException {
        LinkedList<LinkedHashMap<String, String>> rowList = new LinkedList<LinkedHashMap<String, String>>();

        long replyStatusId;
        long retweeStatustId;
        long statusId = status.getId();
//        long statusCreatedDtLong = (status.getCreatedAt()).getTime();
        User user = status.getUser();

        LinkedHashMap<String, String> currRowMap = new LinkedHashMap<String, String>();

        currRowMap.put("statusFilterType", filterType);
        currRowMap.put("statusOriginalUserFilter", orgUserFilter);

        currRowMap.put("statusDerivedUserFilter", derivedUserFilter);

        currRowMap.put("systemCreateDt", new SimpleDateFormat("dd/MM/yyyy-HH:mm:ss").format(new Date(System.currentTimeMillis())));

        currRowMap.put("statusId", Long.toString(status.getId()));
        currRowMap.put("statusUserId", Long.toString(status.getUser().getId()));

        currRowMap.put("statusCreatedAt", status.getCreatedAt().toString());

        currRowMap.put("statusRefStatusId", String.valueOf(refStatusId));
        currRowMap.put("statusLang", status.getLang());
        currRowMap.put("statusSource", status.getSource());
        currRowMap.put("statusText", status.getText());
        currRowMap.put("statusFavoriteCount", Integer.toString(status.getFavoriteCount()));
        currRowMap.put("statusRetweetCount", Integer.toString(status.getRetweetCount()));

        currRowMap.put("statusCurrentUserRetweetId", Long.toString(status.getCurrentUserRetweetId()));
        currRowMap.put("statusInReplyToUserId", Long.toString(status.getInReplyToUserId()));
        replyStatusId = status.getInReplyToStatusId();
        currRowMap.put("statusInReplyToStatusId", Long.toString(replyStatusId));
        if (replyStatusId != -1) {
            currRowMap.put("statusIsReply", "true");
        } else {
            currRowMap.put("statusIsReply", "false");
        }

        currRowMap.put("statusInReplyToScreenName", status.getInReplyToScreenName());
        currRowMap.put("statusIsFavorited", Boolean.toString(status.isFavorited()));
        currRowMap.put("statusIsPossiblySensitive", Boolean.toString(status.isPossiblySensitive()));
        currRowMap.put("statusIsRetweet", Boolean.toString(status.isRetweet()));
        currRowMap.put("statusIsRetweeted", Boolean.toString(status.isRetweeted()));
        currRowMap.put("statusIsTruncated", Boolean.toString(status.isTruncated()));
        Status retweetStatus = status.getRetweetedStatus();
        /**
         * Add retweetStatusId if available
         */
        if (retweetStatus == null) {
            currRowMap.put("statusRetweetedStatusId", null);
        } else {
            retweeStatustId = retweetStatus.getId();

            currRowMap.put("statusRetweetedStatusId", Long.toString(retweeStatustId));

        }
        /**
         * Location related details of Status
         */
        if (status.getGeoLocation() == null) {
            currRowMap.put("statusLatitude", null);
            currRowMap.put("statusLongitude", null);

        } else {
            currRowMap.put("statusLatitude", Double.toString(status.getGeoLocation().getLatitude()));
            currRowMap.put("statusLongitude", Double.toString(status.getGeoLocation().getLongitude()));

        }
        if (status.getPlace() == null) {
            currRowMap.put("statusPlaceCountry", null);
            currRowMap.put("statusPlaceCountryCode", null);
            currRowMap.put("statusPlaceName", null);
            currRowMap.put("statusPlaceId", null);
            currRowMap.put("statusPlaceStreetAddress", null);
        } else {
            currRowMap.put("statusPlaceCountry", status.getPlace().getCountry());
            currRowMap.put("statusPlaceCountryCode", status.getPlace().getCountryCode());
            currRowMap.put("statusPlaceName", status.getPlace().getFullName());
            currRowMap.put("statusPlaceId", status.getPlace().getId());
            currRowMap.put("statusPlaceStreetAddress", status.getPlace().getStreetAddress());
        }

        /**
         *
         * Status Bulk parameters
         *
         *
         */
        if (status.getContributors() != null) {
            int i = 0;
            for (long c : status.getContributors()) {
                currRowMap.put("statusContributors_" + i, Long.toString(c));
                i++;
            }
            getNullValueMap(currRowMap, "statusContributors_", i);
        } else {

            getNullValueMap(currRowMap, "statusContributors_", 0);
        }
        if (status.getMediaEntities() != null) {
            int i = 0;
            for (MediaEntity m : status.getMediaEntities()) {
                currRowMap.put("statusMediaEntities_" + i, m.getMediaURL());
                i++;
            }
            getNullValueMap(currRowMap, "statusMediaEntities_", i);
        } else {
            getNullValueMap(currRowMap, "statusMediaEntities_", 0);
        }
        if (status.getSymbolEntities() != null) {
            int i = 0;
            for (SymbolEntity s : status.getSymbolEntities()) {
                currRowMap.put("statusSymbolEntities_" + i, s.getText());
                i++;
            }
            getNullValueMap(currRowMap, "statusSymbolEntities_", i);
        } else {
            getNullValueMap(currRowMap, "statusSymbolEntities_", 0);
        }
        if (status.getURLEntities() != null) {
            int i = 0;
            for (URLEntity u : status.getURLEntities()) {
                currRowMap.put("statusURLEntities_" + i, u.getText());
                i++;
            }
            getNullValueMap(currRowMap, "statusURLEntities_", i);
        } else {
            getNullValueMap(currRowMap, "statusURLEntities_", 0);
        }
        if (status.getUserMentionEntities() != null) {
            int i = 0;
            for (UserMentionEntity u : status.getUserMentionEntities()) {
                currRowMap.put("statusUserMentionScreenName_" + i, u.getScreenName());

                i++;
            }
            getNullValueMap(currRowMap, "statusUserMentionScreenName_", i);

        } else {
            getNullValueMap(currRowMap, "statusUserMentionScreenName_", 0);

        }
        if (status.getUserMentionEntities() != null) {
            int i = 0;
            for (UserMentionEntity u : status.getUserMentionEntities()) {
                currRowMap.put("statusUserMentionId_" + i, Long.toString(u.getId()));

                i++;
            }
            getNullValueMap(currRowMap, "statusUserMentionId_", i);

        } else {
            getNullValueMap(currRowMap, "statusUserMentionId_", 0);

        }
        if (status.getScopes() != null) {
            if (status.getScopes().getPlaceIds() != null) {
                int i = 0;
                for (String place : status.getScopes().getPlaceIds()) {
                    currRowMap.put("statusScopePlaceIds_" + i, place);

                    i++;
                }
                getNullValueMap(currRowMap, "statusScopePlaceIds_", i);
            } else {
                getNullValueMap(currRowMap, "statusScopePlaceIds_", 0);
            }
        }

        /**
         * Normalization of Hashtag list
         */
        LinkedList<String> hashTagList = new LinkedList<String>();
        for (HashtagEntity h : status.getHashtagEntities()) {
            hashTagList.add(h.getText());
        }
        LinkedList<LinkedHashMap<String, String>> normalizedMap = DMController.getNormalizedMap(currRowMap, hashTagList, "statusHashtagEntity");
        rowList.addAll(normalizedMap);
        normalizedMap.clear();

        /**
         * Writing the extracted status details to file
         */
        if (batchName.equals(FILTER_TYPE_TRACKER) && (orgUserFilter == null || orgUserFilter.isEmpty()
                || orgUserFilter.equals(ORIGINAL_USER_TYPE_KEYWORD))) {
            this.writeRecord(rowList, "REJECTED-STATUS");
            /**
             * Pulkit: - Disabling following condition will allow duplicates
             */

//        } else if (!DMController.lookupCacheFileStatus(String.valueOf(status.getId()))) {
        } else {
            this.writeRecord(rowList, "STATUS");

        }
        currRowMap.clear();

        rowList.clear();
        /**
         * Get details of RetweetedStatus recursively
         *
         *
         */
        if (retweetStatus != null && this.referenceLevel > 0) {
            /**
             * Since Batch Processor works on standard/verified twitter users,
             * retweet user should also be followed
             */
            Long userId = retweetStatus.getUser().getId();
            String userScreenName = retweetStatus.getUser().getScreenName();
            registerUser(userScreenName, userId, orgUserFilter);

            this.getStatusDetails(retweetStatus, filterType, derivedUserFilter, statusId, orgUserFilter);
            referenceLevel--;

        }

        this.getUserDetails(user, statusId, filterType, derivedUserFilter, orgUserFilter);

    }

    private void registerUser(String userScreenName, long userId, String orgUserName) {
        LinkedList<String> bufferList = DMController.batchNameToRegisteredUserListMap.get(batchName);
        DMController.derToOrgUserNameMap.put(userScreenName, orgUserName);

        if (bufferList == null) {
            DMController.batchNameToRegisteredUserListMap.put(batchName, new LinkedList<String>());
        }
//        if (!DMController.twitterRegisteredUserScreenNameToIdMap.containsKey(userScreenName)) {
//            DMController.twitterRegisteredUserScreenNameToIdMap.put(userScreenName, userId);
//        }
        if (!DMController.batchNameToRegisteredUserListMap.get(batchName).contains(userScreenName)) {
            DMController.batchNameToRegisteredUserListMap.get(batchName).add(userScreenName);
        }

        if (!DMController.twitterRegisteredUserScreenNameToIdMap.containsKey(userScreenName)) {
            DMController.twitterRegisteredUserScreenNameToIdMap.put(userScreenName, userId);
        }
    }

    @Override
    public void onDeletionNotice(long l, long l1) {
    }

    @Override
    public void onFriendList(long[] longs) {
    }

    @Override
    public void onFavorite(User user, User user1, Status status) {
    }

    @Override
    public void onUnfavorite(User user, User user1, Status status) {
    }

    @Override
    public void onFollow(User user, User user1) {
    }

    @Override
    public void onUnfollow(User user, User user1) {
    }

    @Override
    public void onDirectMessage(DirectMessage dm) {
    }

    @Override
    public void onUserListMemberAddition(User user, User user1, UserList ul) {
    }

    @Override
    public void onUserListMemberDeletion(User user, User user1, UserList ul) {
    }

    @Override
    public void onUserListSubscription(User user, User user1, UserList ul) {
    }

    @Override
    public void onUserListUnsubscription(User user, User user1, UserList ul) {
    }

    @Override
    public void onUserListCreation(User user, UserList ul) {
    }

    @Override
    public void onUserListUpdate(User user, UserList ul) {
    }

    @Override
    public void onUserListDeletion(User user, UserList ul) {
    }

    @Override
    public void onUserProfileUpdate(User user) {
    }

    @Override
    public void onBlock(User user, User user1) {
    }

    @Override
    public void onUnblock(User user, User user1) {
    }

}
