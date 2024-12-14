package MineOnline.dbscan;

import MineOnline.grid.SixteenGrid;
import MineOnline.common.Cluster;
import MineOnline.common.ClusterList;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.*;

public class GlobalDbscanPartition implements AggregateFunction<
        ClusterList,
        Map<Long, ClusterList>,
        List<Cluster>> {

    public double lg;
    public int node;

    public GlobalDbscanPartition(double lg, int node) {
        this.lg = lg;
        this.node = node;
    }

    @Override
    public Map<Long, ClusterList> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<Long, ClusterList> add(ClusterList c, Map<Long, ClusterList> accumulator) {
        accumulator.put(c.getSmallPartition(),c);
        return accumulator;
    }

    @Override
    public List<Cluster> getResult(Map<Long, ClusterList> accumulator) {
        List<List<FinalPoint>> cluster = new ArrayList<>();

        int count = 0;
        boolean merge = false;
        SixteenGrid sixteenGrid = new SixteenGrid(lg);

        List<Cluster> result = new ArrayList<>();
        List<String> res;
        Cluster objs;

        List<Long> closeIDs;
        List<Cluster> closeCluster;
        List<FinalPoint> intersection;
        long clusterid1,clusterid2;


        Map<Long, ClusterList> map = new TreeMap<>(accumulator);
        Map<Long, ClusterList> map2 = new TreeMap<>(accumulator);

//        for (Map.Entry<Long, List<List<FinalPoint>>> entry : map2.entrySet()) {
//            System.out.println(entry.getKey() + " " + entry.getValue());
//        }


        for (Map.Entry<Long, ClusterList> entry : map2.entrySet()) {
//            计算当前key分区的相邻分区
            closeIDs = sixteenGrid.bigCloseId(entry.getKey());
//            获得当前key分区的簇
            List<Cluster> l2 = map.get(entry.getKey()).getObjs();
            for(Cluster l:l2){
                for(long id:closeIDs){
//                    如果较大相邻分区不存在，跳过
                    if(!map.containsKey(id)){
                        continue;
                    }
//                    得到相邻分区id的簇
                    closeCluster = map.get(id).getObjs();
//                  遍历相邻分区的簇
                    for(Cluster LFP:closeCluster){
//                        满足合并条件
                        if(isMerge(l.getObjs(),LFP.getObjs())){
                            intersection = Intersection(l.getObjs(),LFP.getObjs());
                            Cluster c = new Cluster(intersection, LFP.getBigPartition(), LFP.getSmallPartition());
//                          这里添加up和down信息
                            if(l.isUp() || sixteenGrid.isUp(l.getSmallPartition(),l.getBigPartition(),node)){
                                c.setUp(true);
                                c.setMinUp(l.getMinUp());
                            }
                            if(l.isDown() || sixteenGrid.isDown(l.getSmallPartition(),l.getBigPartition(),node)){
                                c.setDown(true);
                                c.setMinDown(l.getMinDown());
                            }
//                            将当前key分区的簇合并到相邻分区id的簇里
                            map.get(id).getObjs().remove(LFP);


                            map.get(id).getObjs().add(c);

                            merge = true;
                            break;
                        }
                    }
                }
//                没有合并，输出当前key分区的簇
                if(!merge){
//                    System.out.println(entry.getKey());
//                    System.out.println(l);
                    res = new ArrayList<>();
                    objs = new Cluster();
                    objs.setDateTime(l.getObjs().get(0).getDateTime());
//                    for(FinalPoint f:l.getObjs()){
//                        res.add(f.getUserID());
//                    }
                    objs.setObjs(l.getObjs());
                    objs.setBigPartition(l.getBigPartition());
                    if(l.isUp() || sixteenGrid.isUp(l.getSmallPartition(),l.getBigPartition(),node)){
                        objs.setUp(true);
                        objs.setMinUp(l.getMinUp());
                        objs.setIndex(sixteenGrid.indexNum(l.getMinUp()));
                    }
                    if(l.isDown() || sixteenGrid.isDown(l.getSmallPartition(),l.getBigPartition(),node)){
                        objs.setDown(true);
                        objs.setMinDown(l.getMinDown());
                        objs.setIndex(sixteenGrid.indexNum(l.getMinDown()));
                    }
                    result.add(objs);
                }
                merge = false;
            }
        }


        return result;
    }

    public boolean isMerge(List<FinalPoint> list1,List<FinalPoint> list2){
        for(FinalPoint fp:list1){
            if(!fp.isCore){
                continue;
            }
            if(list2.contains(fp)){
                return true;
            }
        }
        for(FinalPoint fp:list2){
            if(!fp.isCore){
                continue;
            }
            if(list1.contains(fp)){
                return true;
            }
        }
        return false;
    }

    public List<FinalPoint> Intersection(List<FinalPoint> list1,List<FinalPoint> list2){
        List<FinalPoint> result = new ArrayList<>();
        result.addAll(list1);
        for(FinalPoint fp:list2){
            if(!result.contains(fp)){
                result.add(fp);
                continue;
            }
            if(fp.isCore){
                result.remove(fp);
                result.add(fp);
            }
        }
        return result;
    }

    @Override
    public Map<Long, ClusterList> merge(Map<Long, ClusterList> longListMap, Map<Long, ClusterList> acc1) {
        return null;
    }
}
