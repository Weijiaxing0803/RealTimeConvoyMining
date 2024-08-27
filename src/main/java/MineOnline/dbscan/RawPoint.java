package MineOnline.dbscan;


//construct point with datetime
public class RawPoint {
    public double lon;//longitude
    public double lat;//latitude
    public long cellID;
    public String dateTime;

    public RawPoint(){

    }


    public RawPoint(double lon, double lat, long cellID, String dateTime) {
        this.lon = lon;
        this.lat = lat;
        this.cellID = cellID;
        this.dateTime = dateTime;
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

    public long getCellID() {
        return cellID;
    }

    public void setCellID(long cellID) {
        this.cellID = cellID;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }


    @Override
    public String toString() {
        return "RawPoint{" +
                "lon=" + lon +
                ", lat=" + lat +
                ", cellID=" + cellID +
                ", dateTime='" + dateTime + '\'' +
                '}';
    }
}
