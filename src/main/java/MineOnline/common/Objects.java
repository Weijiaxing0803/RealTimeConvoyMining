package MineOnline.common;

import java.util.ArrayList;
import java.util.List;

public class Objects {
    public List<String> objs = new ArrayList<>();//存放除了obj以外的，比obj对象ID大的对象集合
    public String dateTime;
    public boolean FullMerge;
    public boolean isUp = false;
    public boolean isDown = false;
    public long bigPartition;
    private long minUp;
    private long minDown;
    private long index;

    public Objects(){}


    public Objects(List<String> objs, String dateTime) {
        this.objs = objs;
        this.dateTime = dateTime;
        this.FullMerge = false;
    }

    public boolean isFullMerge() {
        return FullMerge;
    }

    public void setFullMerge(boolean fullMerge) {
        FullMerge = fullMerge;
    }

    public List<String> getObjs() {
        return objs;
    }

    public void setObjs(List<String> objs) {
        this.objs = objs;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public boolean isUp() {
        return isUp;
    }

    public void setUp(boolean up) {
        isUp = up;
    }

    public boolean isDown() {
        return isDown;
    }

    public void setDown(boolean down) {
        isDown = down;
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

    public long getBigPartition() {
        return bigPartition;
    }

    public void setBigPartition(long bigPartition) {
        this.bigPartition = bigPartition;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "Objects{" +
                "objs=" + objs +
                ", dateTime='" + dateTime + '\'' +
                ", FullMerge=" + FullMerge +
                ", isUp=" + isUp +
                ", isDown=" + isDown +
                ", bigPartition=" + bigPartition +
                ", minUp=" + minUp +
                ", minDown=" + minDown +
                ", index=" + index +
                '}';
    }
}
