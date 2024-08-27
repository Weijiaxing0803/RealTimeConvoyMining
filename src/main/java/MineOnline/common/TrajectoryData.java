package MineOnline.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

// TrajectoryData

public class TrajectoryData {
    public String phone;
    public String dateTime;
    public long gridID;
    public double lon;
    public double lat;
    public long closeID;
    public boolean flag;

    public TrajectoryData(){

    }

    public TrajectoryData(String phone, String dateTime, long gridID, double lon, double lat, long closeID) {
        this.phone = phone;
        this.dateTime = dateTime;
        this.gridID = gridID;
        this.lon = lon;
        this.lat = lat;
        this.closeID = closeID;
    }

    public long getTimeStamp(){
        String dateTime = this.dateTime;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            date = sdf.parse(dateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public long getGridID() {
        return gridID;
    }

    public void setGridID(Long gridID) {
        this.gridID = gridID;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public long getCloseID() {
        return closeID;
    }

    public void setCloseID(long closeID) {
        this.closeID = closeID;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "TrajectoryData{" +
                "phone='" + phone + '\'' +
                ", dateTime='" + dateTime + '\'' +
                ", gridID=" + gridID +
                ", lon=" + lon +
                ", lat=" + lat +
                ", closeID=" + closeID +
                ", flag=" + flag +
                '}';
    }
}
