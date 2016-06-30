/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package batch;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import config.DMConfigurationConstants;
import config.DMConfigurationLoader;
import engine.DMController;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import twitter4j.HashtagEntity;
import twitter4j.MediaEntity;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.ResponseList;
import twitter4j.Status;
import twitter4j.SymbolEntity;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.UserMentionEntity;
import writer.DMTargetWritter;

/**
 *
 * @author baradp
 */
public class DMBatchProcessor extends Thread implements Serializable {

//    public static List<LinkedHashMap<String, String>> resultMap = null;
    private LinkedList<String> userNames = null;
    public Twitter twitter = null;
//    private long batchRunDurationSec;
    private long currSinceId;
    private long currMaxId;

    private int pageId = 1;

    public String batchName;
    public int batchRefreshRateMinute;
    public long batchStartTimeLong;

    private long batchCutOffMinDateLong;
    private long batchCutOffMaxDateLong;
//    private long batchCutOffMaxDateUserNameLong;

    public long records = 0;
    public Long userPrevSinceId;
    public Long userPrevMaxId;
    public static final String REVERSE_PAGINATION = "REVERSE_PAGINATION";
    public static final String INITIAL_PAGINATION = "INITIAL_PAGINATION";
    public static final String FORWARD_PAGINATION = "FORWARD_PAGINATION";
    public static final String FILTER_TYPE_TIMELINE = "BATCH_TIMELINE";

    private String paginationMode;

    public long minStatusDateUserLong;

//    private boolean linkReplyUser = false;
//    private int linkReplyLevel;
    private ConcurrentLinkedHashMap<Long, Long> statusToReplyMap = new ConcurrentLinkedHashMap.Builder<Long, Long>()
            .maximumWeightedCapacity(100).build();
//    private static final long RATE_LIMIT_WAIT_TIME = 15 * 60 * 1000;
    private long rateLimitEncounterTime = 1;
    private boolean rateLimitFlag = false;

//    private DateFormat sdf;
//    private int maxConnectionCount = 179;
//    public DMBatchProcessor(List<String> userNames, Twitter twitter, long batchRunDurationSec, String batchCutOffDate) {
//
//    }
    public void init(Twitter twitter, LinkedHashMap<String, String> batchConfig) throws ParseException {
        String trackStream = batchConfig.get(DMConfigurationConstants.BATCH_TRACK_STREAM);
        this.batchName = batchConfig.get(DMConfigurationConstants.BATCH_NAME);
        String startTime = batchConfig.get(DMConfigurationConstants.BATCH_RUN_START_TIME);

        String runCutoffMinDate = batchConfig.get(DMConfigurationConstants.BATCH_CUTOFF_MIN_DATE);
        String runCutoffMaxDate = batchConfig.get(DMConfigurationConstants.BATCH_CUTOFF_MAX_DATE);
        String maxId = batchConfig.get(DMConfigurationConstants.BATCH_REF_LEVEL);
//        String lookupLevel = batchConfig.get(DMConfigurationConstants.BATCH_LINK_REPLY_LEVEL);
//        String lookupUser = batchConfig.get(DMConfigurationConstants.BATCH_LINK_REPLY_USER);

        String users = batchConfig.get(DMConfigurationConstants.BATCH_USER_NAMES);
        String dateFormat = batchConfig.get(DMConfigurationConstants.BATCH_DATE_FORMAT);
        //Create GMT Timezone
//        sdf = new SimpleDateFormat(dateFormat);
//        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));

        //Initialize run duration
//        if (duration != null && duration.length() > 0) {
//            this.batchRunDurationSec = Long.valueOf(duration);
//
//        } else {
//            this.batchRunDurationSec = 0;
//
//        }
        //Assign value of Stream tracking flag to the corresondign batch Name
//        DMController.batchNameToTrackStreamMap.put(this.batchName, trackStream);
        //Initialize since id
        if (maxId != null && maxId.length() > 0) {
            this.currSinceId = Long.valueOf(maxId);
        } else {
            this.currSinceId = Long.MAX_VALUE;
        }
        //Initialize Refresh Rate

        //Initialize run cut off Min date
        if (runCutoffMinDate != null && runCutoffMinDate.length() > 0) {

            this.batchCutOffMinDateLong = new SimpleDateFormat(dateFormat).parse(runCutoffMinDate).getTime();
        } else {
            this.batchCutOffMinDateLong = 0;
        }
        //Initialize batch Start time
        if (startTime != null && startTime.length() > 0) {

            this.batchStartTimeLong = new SimpleDateFormat(dateFormat).parse(startTime).getTime();
        } else {
            this.batchStartTimeLong = System.currentTimeMillis();
        }
        //Initialize run cut off Max date

        if (runCutoffMaxDate != null && runCutoffMaxDate.length() > 0) {

            this.batchCutOffMaxDateLong = new SimpleDateFormat(dateFormat).parse(runCutoffMaxDate).getTime();
        } else {
            this.batchCutOffMaxDateLong = Long.MAX_VALUE;
        }
        // initialize user names
        StringTokenizer tokens = new StringTokenizer(users, ",");
        this.userNames = new LinkedList<String>();
        while (tokens.hasMoreTokens()) {
            this.userNames.add(tokens.nextToken());
        }

        this.twitter = twitter;
    }

    @Override

    public void run() {
        try {
            DMTargetWritter.log("INFO: [" + batchName + "] Execution started (Thread Id-" + Thread.currentThread().getId() + ")");
            String threadStatus = "";

            if (DMController.batchNameToInquiredUserListMap.get(batchName) == null) {
                DMController.batchNameToInquiredUserListMap.put(batchName, this.userNames);
            }

            LinkedList<String> bufferList = new LinkedList<String>();
            bufferList.addAll(this.userNames);
            for (String userName : bufferList) {
                try {
                    this.processUserTimeline(userName);
                } catch (IOException ex) {
                    DMTargetWritter.log(ex);
                } catch (InterruptedException ex) {
                    DMTargetWritter.log(ex);
                }

                String pagingMode = DMController.userNameToPagingModeMap.get(userName);
//
//            if (prevMinDateLong == null || (prevMinDateLong != null && prevMinDateLong > this.minStatusDateUserLong)) {
                threadStatus += " [" + userName + " = " + pagingMode + "]";
//                DMController.userFilterToCutOffMinDateLong.put(userName, this.minStatusDateUserLong);
//            }
            }
            DMTargetWritter.log("INFO: [" + batchName + "] Execution completed (Thread Id-" + Thread.currentThread().getId()
                    + ") Paging mode for each user in this batch are as following" + threadStatus);
            DMController.unscheduleFwdPageBatches();

        } catch (Exception e) {
            DMTargetWritter.log(e);
        }
    }

    private synchronized Twitter handleStatusListTimelineRateLimit(TwitterException e) {
        int minResetTime = 899;
        DMTargetWritter.log("Status list TimeLine Rate limit hit, Fetching availalbe connection from pool");
        if (e != null) {
            DMTargetWritter.log(e);
        }
        try {

            for (Twitter twitterConn : DMController.twitterConnectionPool) {

                RateLimitStatus rateLimit = null;
                rateLimit = twitterConn.getRateLimitStatus().get("/statuses/user_timeline");
                if (rateLimit != null) {
                    minResetTime = Math.min(minResetTime, rateLimit.getSecondsUntilReset());
                }
                String expiredAccessToken = twitter.getConfiguration().getOAuthAccessToken();
                String issuedAccessToken = twitterConn.getConfiguration().getOAuthAccessToken();

                if (rateLimit != null && rateLimit.getRemaining() > 0 && !expiredAccessToken.equalsIgnoreCase(issuedAccessToken)) {
                    DMTargetWritter.log("Changing Twitter connection \n\t\tExhausted_Authentication_Token:" + expiredAccessToken
                            + "\n\t\tIssued_Authentication_Token:" + issuedAccessToken);
                    return twitterConn;
                }
            }
        } catch (TwitterException ex) {
            try {
                if (ex.getRateLimitStatus() != null) {
                    DMTargetWritter.log(" Thread [" + Thread.currentThread().getId() + "] need to sleep for " + ex.getRateLimitStatus().getSecondsUntilReset() + " second.");
                } else {
                    DMTargetWritter.log(ex);
                }

                Thread.sleep(ex.getRateLimitStatus().getSecondsUntilReset() * 1000);
            } catch (Exception ex1) {
                DMTargetWritter.log(ex1);
            }
        }

        try {
            DMTargetWritter.log("  Thread [" + Thread.currentThread().getId() + "] need to sleep for " + minResetTime + " second.");
            Thread.sleep(minResetTime * 1000);
        } catch (InterruptedException ex) {
            DMTargetWritter.log(ex);
        }
        return handleStatusListTimelineRateLimit(null);
    }

    private synchronized Twitter handleStatusListLookupRateLimit(TwitterException e) {
        int minResetTime = 899;
        DMTargetWritter.log("Status List lookup Rate limit hit, Fetching availalbe connection from pool");
        if (e != null) {
            DMTargetWritter.log(e);
        }
        try {

            for (Twitter twitterConn : DMController.twitterConnectionPool) {

                RateLimitStatus rateLimit = null;
                rateLimit = twitterConn.getRateLimitStatus().get("/statuses/lookup");
                minResetTime = Math.min(minResetTime, rateLimit.getSecondsUntilReset());
                String expiredAccessToken = twitter.getConfiguration().getOAuthAccessToken();
                String issuedAccessToken = twitterConn.getConfiguration().getOAuthAccessToken();

                if (rateLimit.getRemaining() > 0 && !expiredAccessToken.equalsIgnoreCase(issuedAccessToken)) {
                    DMTargetWritter.log("Changing Twitter connection \n\t\tExhausted_Authentication_Token:" + expiredAccessToken
                            + "\n\t\tIssued_Authentication_Token:" + issuedAccessToken);
                    return twitterConn;
                }
            }
        } catch (TwitterException ex) {
            try {
                if (ex.getRateLimitStatus() != null) {
                    DMTargetWritter.log(" Thread " + this.getId() + " need to sleep for " + ex.getRateLimitStatus().getSecondsUntilReset() + " second. ");
                } else {
                    DMTargetWritter.log(" Unknown Twitter Exception:[" + ex.toString());
                }

                Thread.sleep(ex.getRateLimitStatus().getSecondsUntilReset() * 1000);
                DMTargetWritter.log(" Thread " + this.getId() + " is now notified");
            } catch (Exception ex1) {
                DMTargetWritter.log(ex1);
            }
        }

        try {
            DMTargetWritter.log(" Thread [" + Thread.currentThread().getId() + "] need to sleep for " + minResetTime + " second.");
            Thread.sleep(minResetTime * 1000);
        } catch (InterruptedException ex) {
            DMTargetWritter.log(ex);
        }

        return handleStatusListLookupRateLimit(null);
    }

    private synchronized Twitter handleSearchTweetsRateLimit(TwitterException e) {
        int minResetTime = 899;
        DMTargetWritter.log("Search Tweet Rate limit hit, Fetching availalbe connection from pool");
        if (e != null) {
            DMTargetWritter.log(e);
        }
        try {

            for (Twitter twitterConn : DMController.twitterConnectionPool) {

                RateLimitStatus rateLimit = null;
                rateLimit = twitterConn.getRateLimitStatus().get("/search/tweets");
                minResetTime = Math.min(minResetTime, rateLimit.getSecondsUntilReset());
                String expiredAccessToken = twitter.getConfiguration().getOAuthAccessToken();
                String issuedAccessToken = twitterConn.getConfiguration().getOAuthAccessToken();

                if (rateLimit.getRemaining() > 0 && !expiredAccessToken.equalsIgnoreCase(issuedAccessToken)) {
                    DMTargetWritter.log("Changing Twitter connection \n\t\tExhausted_Authentication_Token:" + expiredAccessToken
                            + "\n\t\tIssued_Authentication_Token:" + issuedAccessToken);
                    return twitterConn;
                }
            }
        } catch (TwitterException ex) {
            try {
                if (ex.getRateLimitStatus() != null) {
                    DMTargetWritter.log(" Thread [" + Thread.currentThread().getId() + "] need to sleep for " + ex.getRateLimitStatus().getSecondsUntilReset() + " second.");
                } else {
                    DMTargetWritter.log(" Unknown Twitter Exception:[" + ex.toString());
                }

                Thread.sleep(ex.getRateLimitStatus().getSecondsUntilReset() * 1000);
            } catch (Exception ex1) {
                DMTargetWritter.log(ex1);
            }
        }

        try {
            DMTargetWritter.log(" Thread [" + Thread.currentThread().getId() + "] need to sleep for " + minResetTime + " second.");
            Thread.sleep(minResetTime * 1000);
        } catch (InterruptedException ex) {
            DMTargetWritter.log(ex);
        }

        return handleSearchTweetsRateLimit(null);
    }

    private int processUserTimeline(String orgUserFilterName) throws IOException, InterruptedException {

//        int linkReplyLevelBuffer = this.linkReplyLevel;
//        Paging paging = new Paging(pageId, 100, 1, maxStatusId - 1);
        long currentTime = System.currentTimeMillis();
        long searchResultCount = 0;
        Query query = new Query("from:" + orgUserFilterName);
        query.setCount(100);
        /**
         * Extract Batch Mode from the Map object of Controller
         *
         * If it is null then we are in initial mode.
         */
        this.userPrevSinceId = DMController.userNameToPrevSinceIdMap.get(orgUserFilterName);
        this.userPrevMaxId = DMController.userNameToPrevMaxIdMap.get(orgUserFilterName);
        this.paginationMode = DMController.userNameToPagingModeMap.get(orgUserFilterName);
        if (this.paginationMode == null || this.paginationMode == INITIAL_PAGINATION) {
            query.setMaxId(Long.MAX_VALUE);
            this.currMaxId = 0;
            this.currSinceId = Long.MAX_VALUE;
        } else if (this.paginationMode == REVERSE_PAGINATION) {
            query.setMaxId(this.userPrevSinceId - 1);
            if (this.userPrevMaxId != null) {
                this.currMaxId = this.userPrevMaxId;
            } else {
                DMTargetWritter.log(new Exception("During Reverse Paginatation prevMaxId is null for username" + orgUserFilterName));
            }
            this.currSinceId = this.userPrevSinceId;
        } else if (this.paginationMode == FORWARD_PAGINATION) {
            return 0;
        }

//        long batchStopTimeLong;
//        if (this.batchRunDurationSec > 0) {
//            batchStopTimeLong = this.batchStartTimeLong + this.batchRunDurationSec * 1000;
//        } else {
//            batchStopTimeLong = Long.MAX_VALUE;
//        }
//        this.minStatusDateUserLong = Long.MAX_VALUE;
        long currentStatusDateLong = this.batchCutOffMinDateLong + 1;
        boolean firstIter = true;

        int counter = 0;
        try {
            while ((searchResultCount != 0 && searchResultCount % 100 == 0) || firstIter) {

                firstIter = false;
                QueryResult queryResult = null;

                queryResult = this.twitter.search(query);

                List<Status> statusList = queryResult.getTweets();
                searchResultCount = statusList.size();

                for (Status status : statusList) {
                    currentStatusDateLong = status.getCreatedAt().getTime();

                    if (currentStatusDateLong < this.batchCutOffMinDateLong || currentStatusDateLong > this.batchCutOffMaxDateLong) {
                        DMController.userNameToPagingModeMap.put(orgUserFilterName, FORWARD_PAGINATION);

                        return 0;
                    }
//                    this.minStatusDateUserLong = currentStatusDateLong;
                    currSinceId = Math.min(status.getId(), currSinceId);
                    currMaxId = Math.max(status.getId(), currMaxId);
                    this.getStatusDetails(status, orgUserFilterName, -1);

                    registerUser(status.getUser().getScreenName(), status.getUser().getId(), orgUserFilterName);
                    currentTime = System.currentTimeMillis();
                    query.setMaxId(currSinceId);
                    counter++;
                }
            }
            DMController.userNameToPrevMaxIdMap.put(orgUserFilterName, currMaxId);
            DMController.userNameToPrevSinceIdMap.put(orgUserFilterName, currSinceId);

            if (counter == 0) {
                DMController.userNameToPagingModeMap.put(orgUserFilterName, FORWARD_PAGINATION);
            } else if ((this.paginationMode != null && this.paginationMode == REVERSE_PAGINATION) || this.userPrevSinceId == null) {
                DMController.userNameToPagingModeMap.put(orgUserFilterName, REVERSE_PAGINATION);

            }

        } catch (TwitterException ex) {
            this.twitter = this.handleSearchTweetsRateLimit(ex);
            this.processUserTimeline(orgUserFilterName);
        }

        /**
         * If lookup user count < 100 then initiate force lookup
         *
         */
        if (statusToReplyMap != null && statusToReplyMap.size() > 0 && statusToReplyMap.size() < 100) {
            this.forceLookupReplyStatus(orgUserFilterName);
        }
        return 0;
    }

    private void getUserDetails(User user, long tweetId, String userNameFilter) throws IOException {
        LinkedHashMap<String, String> currRowMap = new LinkedHashMap<String, String>();
        currRowMap.put("userFilterType", FILTER_TYPE_TIMELINE);

        currRowMap.put("userOriginalUserFilter", userNameFilter);

        currRowMap.put("userDerivedUserFilter", userNameFilter);

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
            currRowMap.putAll(getNullValueMap("userDescriptionURLEntities_", i));
        } else {
            currRowMap.putAll(getNullValueMap("userDescriptionURLEntities_", 0));
        }
        /**
         * Write extracted record to target
         */
        LinkedList<LinkedHashMap<String, String>> rowList = new LinkedList<LinkedHashMap<String, String>>();
        rowList.add(currRowMap);
        this.writeRecord(rowList, "USER");
        /**
         * Clear the map after writing to target
         *
         */
        currRowMap.clear();
        rowList.clear();

    }

    private void writeRecord(LinkedList<LinkedHashMap<String, String>> currRowMapList, String keyword) throws IOException {
        String outputFileName = DMConfigurationLoader.outputDirectory + "[" + batchName + "-" + keyword + "]";
        String dateString = DMTargetWritter.refreshTimeDateString;
        if (dateString != null) {
            outputFileName += dateString;
        }
        DMTargetWritter.writeRecordList(currRowMapList, outputFileName);
        this.records++;

    }

    private LinkedHashMap<String, String> getNullValueMap(String keyword, int startIndex) {
        LinkedHashMap<String, String> nullValueMap = new LinkedHashMap<String, String>();
        for (int index = startIndex; index < 12; index++) {
            nullValueMap.put(keyword + index, null);
        }
        return nullValueMap;
    }

    private void getStatusDetails(Status status, String filterName, long refStatusId) throws IOException {
        LinkedList<LinkedHashMap<String, String>> rowList = new LinkedList<LinkedHashMap<String, String>>();
        long replyStatusId;
        long retweeStatustId;
        long statusId = status.getId();
//        long statusCreatedDtLong = (status.getCreatedAt()).getTime();
        User user = status.getUser();

//        if (statusCreatedDtLong <= this.batchCutOffMaxDateLong) {
        LinkedHashMap<String, String> currRowMap = new LinkedHashMap<String, String>();
        currRowMap.put("statusFilterType", FILTER_TYPE_TIMELINE);
        currRowMap.put("statusOriginalUserFilter", filterName);
        DMController.derToOrgUserNameMap.put(filterName, filterName);
        currRowMap.put("statusDerivedUserFilter", filterName);

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
            currRowMap.putAll(getNullValueMap("statusContributors_", i));
        } else {
            currRowMap.putAll(getNullValueMap("statusContributors_", 0));
        }
        if (status.getMediaEntities() != null) {
            int i = 0;
            for (MediaEntity m : status.getMediaEntities()) {
                currRowMap.put("statusMediaEntities_" + i, m.getMediaURL());
                i++;
            }
            currRowMap.putAll(getNullValueMap("statusMediaEntities_", i));
        } else {
            currRowMap.putAll(getNullValueMap("statusMediaEntities_", 0));
        }
        if (status.getSymbolEntities() != null) {
            int i = 0;
            for (SymbolEntity s : status.getSymbolEntities()) {
                currRowMap.put("statusSymbolEntities_" + i, s.getText());
                i++;
            }
            currRowMap.putAll(getNullValueMap("statusSymbolEntities_", i));
        } else {
            currRowMap.putAll(getNullValueMap("statusSymbolEntities_", 0));
        }
        if (status.getURLEntities() != null) {
            int i = 0;
            for (URLEntity u : status.getURLEntities()) {
                currRowMap.put("statusURLEntities_" + i, u.getText());
                i++;
            }
            currRowMap.putAll(getNullValueMap("statusURLEntities_", i));
        } else {
            currRowMap.putAll(getNullValueMap("statusURLEntities_", 0));
        }
        if (status.getUserMentionEntities() != null) {
            int i = 0;
            for (UserMentionEntity u : status.getUserMentionEntities()) {
                currRowMap.put("statusUserMentionScreenName_" + i, u.getScreenName());

                i++;
            }
            currRowMap.putAll(getNullValueMap("statusUserMentionScreenName_", i));

        } else {
            currRowMap.putAll(getNullValueMap("statusUserMentionScreenName_", 0));

        }
        if (status.getUserMentionEntities() != null) {
            int i = 0;
            for (UserMentionEntity u : status.getUserMentionEntities()) {
                currRowMap.put("statusUserMentionId_" + i, Long.toString(u.getId()));

                i++;
            }
            currRowMap.putAll(getNullValueMap("statusUserMentionId_", i));

        } else {
            currRowMap.putAll(getNullValueMap("statusUserMentionId_", 0));

        }
        if (status.getScopes() != null) {
            if (status.getScopes().getPlaceIds() != null) {
                int i = 0;
                for (String place : status.getScopes().getPlaceIds()) {
                    currRowMap.put("statusScopePlaceIds_" + i, place);

                    i++;
                }
                currRowMap.putAll(getNullValueMap("statusScopePlaceIds_", i));
            } else {
                currRowMap.putAll(getNullValueMap("statusScopePlaceIds_", 0));
            }
        }

        /**
         * Normalization of Hashtag list
         */
        LinkedList<String> hashTagList = new LinkedList<String>();
        for (HashtagEntity h : status.getHashtagEntities()) {
            hashTagList.add(h.getText());
        }
        rowList.addAll(DMController.getNormalizedMap(currRowMap, hashTagList, "statusHashtagEntity"));
        //        if (status.getHashtagEntities() != null) {
        //            int i = 0;
        //            for (HashtagEntity h : status.getHashtagEntities()) {
        //                currRowMap.put("statusHashtagEntities_" + i, h.getText());
        //                i++;
        //            }
        //            currRowMap.putAll(getNullValueMap("statusHashtagEntities_", i));
        //        } else {
        //            currRowMap.putAll(getNullValueMap("statusHashtagEntities_", 0));
        //        }
        /**
         * Writing the extracted status details to file
         */
        /**
         * Pulkit: - Disabling following condition will allow duplicates
         */

//        if (!DMController.lookupCacheFileStatus(String.valueOf(status.getId()))) {
        this.writeRecord(rowList, "STATUS");
//        }
        currRowMap.clear();
        rowList.clear();

        /**
         * Get details of RetweetedStatus recursively
         *
         *
         */
        if (retweetStatus != null) {
        /**
         * Pulkit: - Disabling following condition will allow duplicates
         */
//            if (!DMController.lookupCacheFileStatus(String.valueOf(retweetStatus.getId()))) {

            /**
             * Since Batch Processor works on standard/verified twitter users,
             * retweet user should also be followed
             */
            Long userId = retweetStatus.getUser().getId();
            String userScreenName = retweetStatus.getUser().getScreenName();

            registerUser(userScreenName, userId, filterName);
            this.getStatusDetails(retweetStatus, filterName, statusId);
//            }
        }
        /**
         * Lookup the reply user
         *
         */
//        if (!processedUsers.contains(user.getScreenName())) {
        if (!DMController.lookupCacheFileUser(user.getScreenName())) {
            this.getUserDetails(user, statusId, filterName);
        }
//        }
        if (replyStatusId != -1) {

            lookupReplyStatus(replyStatusId, statusId, filterName);
        }
//        }
//        return statusCreatedDtLong;
    }

    private void registerUser(String userScreenName, long userId, String orgUserName) {
        DMController.derToOrgUserNameMap.put(userScreenName, orgUserName);

        LinkedList<String> bufferList = DMController.batchNameToRegisteredUserListMap.get(batchName);
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

    private void lookupReplyStatus(long replyStatusId, long statusId, String userNameFilter) throws IOException {
        ConcurrentLinkedHashMap<Long, Long> statusToReplyMapBuffer = new ConcurrentLinkedHashMap.Builder<Long, Long>()
                .maximumWeightedCapacity(100).build();
        if (stackReplyStatus(replyStatusId, statusId) == 100) {
//            if (this.acquireConnetion()) {
            long[] users = new long[100];
            int index = 0;
            statusToReplyMapBuffer.putAll(statusToReplyMap);

            for (Map.Entry<Long, Long> entry : statusToReplyMapBuffer.entrySet()) {
                users[index] = entry.getKey();
                statusToReplyMap.remove(entry.getKey());
                index++;
            }
            ResponseList<Status> replyStatusList = null;
            if (twitter != null) {
                try {
                    replyStatusList = twitter.lookup(users);
                } catch (TwitterException ex) {
                    twitter = this.handleStatusListLookupRateLimit(ex);
                    lookupReplyStatus(replyStatusId, statusId, userNameFilter);

                }
                if (replyStatusList != null) {
                    for (Status stackReplyStatus : replyStatusList) {
                        long stackRefStatusId = statusToReplyMapBuffer.get(stackReplyStatus.getId());
                        if (!DMController.lookupCacheFileStatus(String.valueOf(stackReplyStatus.getId()))) {

                            this.getStatusDetails(stackReplyStatus, userNameFilter, stackRefStatusId);
                        }
                    }
                }
                statusToReplyMapBuffer.clear();

            }
//            }
        }
    }

    private void forceLookupReplyStatus(String userNameFilter) throws IOException {
        ConcurrentLinkedHashMap<Long, Long> statusToReplyMapBuffer = new ConcurrentLinkedHashMap.Builder<Long, Long>()
                .maximumWeightedCapacity(100).build();
        statusToReplyMapBuffer.putAll(statusToReplyMap);
        long[] users = new long[statusToReplyMapBuffer.size()];
        int index = 0;
        for (Map.Entry entry : statusToReplyMapBuffer.entrySet()) {
            users[index] = ((Long) entry.getKey());
            statusToReplyMap.remove(entry.getKey());

            index++;
        }
        ResponseList<Status> replyStatusList = null;
        if (twitter != null) {
            try {
                replyStatusList = twitter.lookup(users);
            } catch (TwitterException ex) {
                twitter = this.handleStatusListLookupRateLimit(ex);
                this.forceLookupReplyStatus(userNameFilter);
            }
            if (replyStatusList != null) {
                for (Status stackReplyStatus : replyStatusList) {
                    long stackRefStatusId = statusToReplyMapBuffer.get(stackReplyStatus.getId());
                    if (!DMController.lookupCacheFileStatus(String.valueOf(stackReplyStatus.getId()))) {
                        this.getStatusDetails(stackReplyStatus, userNameFilter, stackRefStatusId);
                    }
                }
            }
            statusToReplyMapBuffer.clear();
        }
    }

    private int stackReplyStatus(long replyStatusId, long statusId) {
        statusToReplyMap.put(replyStatusId, statusId);
        return statusToReplyMap.size();
    }

}
