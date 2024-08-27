package MineOnline.dbscan;

import java.util.HashSet;

public class SimpleCluster {

    public HashSet<String> oids;
    public long ID;


    public SimpleCluster() {
//	oids = new HashSet<Integer>();
        oids = new HashSet<String>();
    }


    public void setID(long ID) {
        this.ID = ID;
    }


    public void addObject(String obj) {
        oids.add(obj);
    }

    public HashSet<String> getOids() {
        return oids;
    }

    public long getID() {
        return ID;
    }

    @Override
    public String toString() {
        return "<" + ID + ":" + oids.toString() + ">";
    }
}

