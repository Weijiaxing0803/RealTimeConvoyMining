package MineOnline.dbscan;

import MineOnline.common.TrajectoryData;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


//the detail of localDBSCAN
public class LocalDbscan implements AggregateFunction<
        TrajectoryData,
        List<TrajectoryData>,
        Map<Long,List<List<FinalPoint>>>>{

    public double eps;
    public int minPts;

    public LocalDbscan(double eps,int minPts) {
        this.eps = eps;
        this.minPts = minPts;
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
    public  Map<Long,List<List<FinalPoint>>> getResult(List<TrajectoryData> accumulator) {
        List<List<FinalPoint>> finalPoints = new ArrayList<>();
        Map<Long,List<List<FinalPoint>>> map = new HashMap<>();
        String dateTime = accumulator.get(0).getDateTime();
        long closeID = accumulator.get(0).getCloseID();
        //DBSCAN clustering
        DBSCAN dbscan = new DBSCAN(eps,minPts,dateTime,accumulator);
        finalPoints = dbscan.cluster();
        dbscan.setID_COUNTER(0L);
        map.put(closeID,finalPoints);


        return map;

    }

    @Override
    public List<TrajectoryData> merge(List<TrajectoryData> trajectoryData, List<TrajectoryData> acc1) {
        return null;
    }
}
