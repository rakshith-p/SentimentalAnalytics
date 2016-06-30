/*
 * Contains necessary structure to hold data parsed from from appConfig.json
 * Contains necessary setter and getter to read/modify these values;
 */


public class AppConfigStructure {

	private int countTwitterConnections;
	TwitterConnection connectionPool[];
	public int getCountTwitterConnections(){
		return countTwitterConnections;
	}
	public TwitterConnection[] getConnectionPool(){
		return connectionPool;
	}
	public void setConnectionPool(TwitterConnection[] t){
		this.connectionPool = t;
		countTwitterConnections = this.connectionPool.length;
	}
	
	
	private int countBatchFiles;
	String batchFileList[];
	public int getCountBatchFiles(){
		return countBatchFiles;
	}
	public String[] getBatchFileList(){
		return batchFileList;
	}
	public void setBatchFileList(String[] newBatchFileList){
		this.batchFileList = newBatchFileList;
		countBatchFiles = this.batchFileList.length;
	}
	
	
	private String customCommand;
	private int customCommandIntervalMin;
	private boolean allowParallelDBLoading;
	private long shutdownTimeMinute;
	private String outputDirectory;
	private String directorySeparator;
	private String fieldSeparator;
	private String lineSeparator;
	private int writerBufferKB;
	private int dbsyncIntervalMin;
	private String dbHost;
	private String dbPort;
	private String dbName;
	private String twitterStatusTable;
	private String twitterUserTable;
	private String dbUsername;
	private String dbPassword;
	private String proxyHost;
	private String proxyPort;
	
	
	
	
	public AppConfigStructure(){
		countBatchFiles = 0;
		countTwitterConnections = 0;
	}
	
	
	public String getCustomCommand() {
		return customCommand;
	}
	public void setCustomCommand(String customCommand) {
		this.customCommand = customCommand;
	}
	
	public int getCustomCommandIntervalMin() {
		return customCommandIntervalMin;
	}
	public void setCustomCommandIntervalMin(int customCommandIntervalMin) {
		this.customCommandIntervalMin = customCommandIntervalMin;
	}
	public boolean isAllowParallelDBLoading() {
		return allowParallelDBLoading;
	}
	public void setAllowParallelDBLoading(boolean allowParallelDBLoading) {
		this.allowParallelDBLoading = allowParallelDBLoading;
	}
	public long getShutdownTimeMinute() {
		return shutdownTimeMinute;
	}
	public void setShutdownTimeMinute(long shutdownTimeMinute) {
		this.shutdownTimeMinute = shutdownTimeMinute;
	}
	public String getOutputDirectory() {
		return outputDirectory;
	}
	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}
	public String getDirectorySeparator() {
		return directorySeparator;
	}
	public void setDirectorySeparator(String directorySeparator) {
		this.directorySeparator = directorySeparator;
	}
	public String getFieldSeparator() {
		return fieldSeparator;
	}
	public void setFieldSeparator(String fieldSeparator) {
		this.fieldSeparator = fieldSeparator;
	}
	public String getLineSeparator() {
		return lineSeparator;
	}
	public void setLineSeparator(String lineSeparator) {
		this.lineSeparator = lineSeparator;
	}
	public int getWriterBufferKB() {
		return writerBufferKB;
	}
	public void setWriterBufferKB(int writerBufferKB) {
		this.writerBufferKB = writerBufferKB;
	}
	public int getDbsyncIntervalMin() {
		return dbsyncIntervalMin;
	}
	public void setDbsyncIntervalMin(int dbsyncIntervalMin) {
		this.dbsyncIntervalMin = dbsyncIntervalMin;
	}
	public String getDbHost() {
		return dbHost;
	}
	public void setDbHost(String dbHost) {
		this.dbHost = dbHost;
	}
	public String getDbPort() {
		return dbPort;
	}
	public void setDbPort(String dbPort) {
		this.dbPort = dbPort;
	}
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getTwitterStatusTable() {
		return twitterStatusTable;
	}
	public void setTwitterStatusTable(String twitterStatusTable) {
		this.twitterStatusTable = twitterStatusTable;
	}
	public String getTwitterUserTable() {
		return twitterUserTable;
	}
	public void setTwitterUserTable(String twitterUserTable) {
		this.twitterUserTable = twitterUserTable;
	}
	public String getDbUsername() {
		return dbUsername;
	}
	public void setDbUsername(String dbUsername) {
		this.dbUsername = dbUsername;
	}
	public String getDbPassword() {
		return dbPassword;
	}
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}
	public String getProxyHost() {
		return proxyHost;
	}
	public void setProxyHost(String proxyHost) {
		this.proxyHost = proxyHost;
	}
	public String getProxyPort() {
		return proxyPort;
	}
	public void setProxyPort(String proxyPort) {
		this.proxyPort = proxyPort;
	}
	
	
}
