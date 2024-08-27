package MineOnline.dbscan;

import java.util.Objects;

//the finalPoint is a point in the cluster
public class FinalPoint {

    public long  clusterID;
    public String dateTime;
    public String userID;
    public boolean isCore;

    public FinalPoint(){

    }

    public FinalPoint(long clusterID, String dateTime, String userID, boolean isCore) {
        this.clusterID = clusterID;
        this.dateTime = dateTime;
        this.userID = userID;
        this.isCore = isCore;
    }

    public long getClusterID() {
        return clusterID;
    }

    public void setClusterID(long clusterID) {
        this.clusterID = clusterID;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public boolean isCore() {
        return isCore;
    }

    public void setCore(boolean core) {
        isCore = core;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof FinalPoint)) return false;
        FinalPoint that = (FinalPoint) o;
        return Objects.equals(getUserID(), that.getUserID());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getUserID());
    }

    @Override
    public String toString() {
        return "FinalPoint{" +
                "clusterID=" + clusterID +
                ", dateTime='" + dateTime + '\'' +
                ", userID='" + userID + '\'' +
                ", isCore=" + isCore +
                '}';
    }
}
