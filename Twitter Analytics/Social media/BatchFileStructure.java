
public class BatchFileStructure {

	private String batchRunStartTime;
	private String batchCutOffMaxDate;
	private String batchName;
	private String batchCutOffMinDate;
    private String batchTrackStream;
    private String userNames;
    private String dateFormat;
    private int batchRefLevel;
    //private String DoNotModifyThis;
	public String getBatchRunStartTime() {
		return batchRunStartTime;
	}
	public void setBatchRunStartTime(String batchRunStartTime) {
		this.batchRunStartTime = batchRunStartTime;
	}
	public String getBatchCutOffMaxDate() {
		return batchCutOffMaxDate;
	}
	public void setBatchCutOffMaxDate(String batchCutOffMaxDate) {
		this.batchCutOffMaxDate = batchCutOffMaxDate;
	}
	public String getBatchName() {
		return batchName;
	}
	public void setBatchName(String batchName) {
		this.batchName = batchName;
	}
	public String getBatchCutOffMinDate() {
		return batchCutOffMinDate;
	}
	public void setBatchCutOffMinDate(String batchCutOffMinDate) {
		this.batchCutOffMinDate = batchCutOffMinDate;
	}
	public String getBatchTrackStream() {
		return batchTrackStream;
	}
	public void setBatchTrackStream(String batchTrackStream) {
		this.batchTrackStream = batchTrackStream;
	}
	public String getUserNames() {
		return userNames;
	}
	public void setUserNames(String userNames) {
		this.userNames = userNames;
	}
	public String getDateFormat() {
		return dateFormat;
	}
	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}
	public int getBatchRefLevel() {
		return batchRefLevel;
	}
	public void setBatchRefLevel(int batchRefLevel) {
		this.batchRefLevel = batchRefLevel;
	}
}
