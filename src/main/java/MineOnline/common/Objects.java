package MineOnline.common;

import java.util.ArrayList;
import java.util.List;

//store cluster objects
public class Objects {
    public List<String> objs = new ArrayList<>();
    public String dateTime;
    public boolean FullMerge;

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

    @Override
    public String toString() {
        return "Objects{" +
                "objs=" + objs +
                ", dateTime='" + dateTime + '\'' +
                '}';
    }
}
