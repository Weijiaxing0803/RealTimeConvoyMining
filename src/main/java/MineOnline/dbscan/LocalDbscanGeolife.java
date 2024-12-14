package MineOnline.dbscan;

import MineOnline.common.TrajectoryData;
import MineOnline.grid.GeoLifeGrid;
import MineOnline.common.Cluster;
import MineOnline.common.ClusterList;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.List;

public class LocalDbscanGeolife implements AggregateFunction<
        TrajectoryData,
        List<TrajectoryData>,
        ClusterList>{

    public double eps;
    public int minPts;
    public int partition;

    public LocalDbscanGeolife(double eps, int minPts, int partition) {
        this.eps = eps;
        this.minPts = minPts;
        this.partition = partition;
    }

    @Override
    public List<TrajectoryData> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<TrajectoryData> add(TrajectoryData trajectoryData, List<TrajectoryData> accumulator) {
        accumulator.add(trajectoryData);
        return accumulator;
    }

    @Override
    public ClusterList getResult(List<TrajectoryData> accumulator) {
        List<List<FinalPoint>> finalPoints = new ArrayList<>();
        String dateTime = accumulator.get(0).getDateTime();
        long closeID = accumulator.get(0).getCloseID();
        DBSCAN dbscan = new DBSCAN(eps,minPts,dateTime,accumulator);
        finalPoints = dbscan.cluster();
        dbscan.setID_COUNTER(0L);
        GeoLifeGrid sixteenGrid = new GeoLifeGrid(eps);
        long bigPartition = sixteenGrid.bigPartition(closeID,partition);
        List<Cluster> LC = new ArrayList<>();
        for(List<FinalPoint> l:finalPoints){
            Cluster c = new Cluster(l,bigPartition,closeID);
            LC.add(c);
        }
        ClusterList cluster = new ClusterList(bigPartition,closeID,LC);

        return cluster;

    }

    @Override
    public List<TrajectoryData> merge(List<TrajectoryData> trajectoryData, List<TrajectoryData> acc1) {
        return null;
    }
}
