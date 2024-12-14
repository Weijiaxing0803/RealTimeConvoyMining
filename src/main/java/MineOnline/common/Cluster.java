package MineOnline.common;

import MineOnline.dbscan.FinalPoint;

import java.util.List;

public class Cluster {
    private List<FinalPoint> objs;
    public String dateTime;
    private long bigPartition;
    private long smallPartition;
    private boolean up = false;
    private boolean down = false;
    private long minUp;
    private long minDown;
    private long index;
    private boolean merged = false;

    public  Cluster(){

    }

    public Cluster(List<FinalPoint> objs, long bigPartition, long smallPartition) {
        this.objs = objs;
        this.bigPartition = bigPartition;
        this.smallPartition = smallPartition;
        this.minUp = smallPartition;
        this.minDown = smallPartition;
    }

    public List<FinalPoint> getObjs() {
        return objs;
    }

    public void setObjs(List<FinalPoint> objs) {
        this.objs = objs;
    }

    public long getBigPartition() {
        return bigPartition;
    }

    public void setBigPartition(long bigPartition) {
        this.bigPartition = bigPartition;
    }

    public long getSmallPartition() {
        return smallPartition;
    }

    public void setSmallPartition(long smallPartition) {
        this.smallPartition = smallPartition;
    }

    public boolean isUp() {
        return up;
    }

    public void setUp(boolean up) {
        this.up = up;
    }

    public boolean isDown() {
        return down;
    }

    public void setDown(boolean down) {
        this.down = down;
    }

    public long getMinUp() {
        return minUp;
    }

    public void setMinUp(long minUp) {
        this.minUp = minUp;
    }

    public long getMinDown() {
        return minDown;
    }

    public void setMinDown(long minDown) {
        this.minDown = minDown;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public boolean isMerged() {
        return merged;
    }

    public void setMerged(boolean merged) {
        this.merged = merged;
    }

    @Override
    public String toString() {
        String s = "";
        for(FinalPoint o:objs){
            s += o.getUserID() + "-";
        }
        return "Cluster{" +
                "objs=" + s +
                ", dateTime='" + dateTime + '\'' +
                ", bigPartition=" + bigPartition +
                ", smallPartition=" + smallPartition +
                ", up=" + up +
                ", down=" + down +
                ", minUp=" + minUp +
                ", minDown=" + minDown +
                ", index=" + index +
                '}';
    }
}
