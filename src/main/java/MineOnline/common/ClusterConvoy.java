package MineOnline.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

//candidate convoy :include primary candidate convoy and sub candidate convoy
public class ClusterConvoy {
    private List<Integer> objs = new ArrayList<Integer>();
    private String startTime;
    private String endTime;
    private long lifespan;
    private boolean fullmerged;
    private int count;
    private List<Convoy> SubConvoy = new ArrayList<>();

    public ClusterConvoy(){

    }

    public ClusterConvoy(List<Integer> objs, String startTime, String endTime, long lifespan) {
        this.objs = objs;
        this.startTime = startTime;
        this.endTime = endTime;
        this.lifespan = lifespan;
        fullmerged = false;
        count = 0;
    }

    public List<Convoy> getSubConvoy() {
        return SubConvoy;
    }

    public void setSubConvoy(List<Convoy> subConvoy) {
        SubConvoy = subConvoy;
    }

    public void addSubConvoy(Convoy convoys) {
        SubConvoy.add(convoys);
    }


    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public List<Integer> getObjs() {
        return objs;
    }

    public void setObjs(List<Integer> objs) {
        this.objs = objs;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public boolean isFullmerged() {
        return fullmerged;
    }

    public void setFullmerged(boolean fullmerged) {
        this.fullmerged = fullmerged;
    }

    public boolean jump(int m){
        return (this.size() - count) < m;
    }

    public long lifetime(){
        return (getEndTimeStamp()-getStartTimeStamp()) / lifespan + 1;
    }


    public int size(){
        return objs.size();
    }

    public boolean hasSameObjs(Convoy v){
        List<Integer> objs2 = v.getObjs();
        if(objs.size()==objs2.size() && objs.containsAll(objs2) && objs2.containsAll(objs)){
            return true;
        }
        else
            return false;
    }
    public boolean isSubset(ClusterConvoy v){
        if(v.getObjs().containsAll(objs))
            return true;
        else
            return false;
    }

    public long getStartTimeStamp(){
        String dateTime = this.startTime;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            date = sdf.parse(dateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    public long getEndTimeStamp(){
        String dateTime = this.endTime;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            date = sdf.parse(dateTime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    @Override
    public String toString() {
        String s = "ClusterConvoy{" +
                "objs=" + objs +
                ", startTime='" + startTime + '\'' +
                ", endTime='" + endTime + '\'' +
                '}';

        for(Convoy c:SubConvoy){
            s += c.toString();
        }
        return s;
    }
}
