package MineOnline.common;

import java.util.List;

public class ClusterList {
    private long bigPartition;
    private long smallPartition;
    private List<MineOnline.common.Cluster> objs;
    private boolean up = false;
    private boolean down = false;

    public ClusterList(long bigPartition, long smallPartition, List<MineOnline.common.Cluster> objs) {
        this.bigPartition = bigPartition;
        this.smallPartition = smallPartition;
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

    public List<MineOnline.common.Cluster> getObjs() {
        return objs;
    }

    public void setObjs(List<MineOnline.common.Cluster> objs) {
        this.objs = objs;
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

    @Override
    public String toString() {
        return "ClusterList{" +
                "bigPartition=" + bigPartition +
                ", smallPartition=" + smallPartition +
                ", objs=" + objs +
                '}';
    }
}
