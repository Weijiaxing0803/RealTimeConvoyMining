package MineOnline.dbscan;

import MineOnline.common.Objects;
import MineOnline.grid.GeoLifeGrid;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.*;

//the detail of GlobalDBSCAN for geolife
//The difference lies in the size of the partition
public class GlobalDbscanGeolife implements AggregateFunction<
        Map<Long,List<List<FinalPoint>>>,
        Map<Long,List<List<FinalPoint>>>,
        List<Objects>> {

    public double lg;

    public GlobalDbscanGeolife(double lg) {
        this.lg = lg;
    }

    @Override
    public Map<Long, List<List<FinalPoint>>> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public Map<Long, List<List<FinalPoint>>> add(Map<Long, List<List<FinalPoint>>> map, Map<Long, List<List<FinalPoint>>> accumulator) {
        accumulator.putAll(map);
        return accumulator;
    }

    @Override
    public List<Objects> getResult(Map<Long, List<List<FinalPoint>>> accumulator) {
        List<List<FinalPoint>> cluster = new ArrayList<>();

        int count = 0;
        boolean merge = false;

        //here different with GlobalDBSCAN
        GeoLifeGrid sixteenGrid = new GeoLifeGrid(lg);

        List<Objects> result = new ArrayList<>();
        List<String> res;
        Objects objs;

        List<Long> closeIDs;
        List<List<FinalPoint>> closeCluster;
        List<FinalPoint> intersection;
        long clusterid1,clusterid2;


        Map<Long, List<List<FinalPoint>>> map = new TreeMap<>(accumulator);
        Map<Long, List<List<FinalPoint>>> map2 = new TreeMap<>(accumulator);



        for (Map.Entry<Long, List<List<FinalPoint>>> entry : map2.entrySet()) {

            closeIDs = sixteenGrid.bigCloseId(entry.getKey());
            List<List<FinalPoint>> l2 = map.get(entry.getKey());
            for(List<FinalPoint> l:l2){
                for(long id:closeIDs){
                    if(!map.containsKey(id)){
                        continue;
                    }
                    closeCluster = map.get(id);

                    for(List<FinalPoint> LFP:closeCluster){
                        if(isMerge(l,LFP)){
                            intersection = Intersection(l,LFP);
                            map.get(id).remove(LFP);
                            map.get(id).add(intersection);
                            merge = true;
                            break;
                        }

                    }
                }
                if(!merge){
//                    System.out.println(entry.getKey());
//                    System.out.println(l);
                    res = new ArrayList<>();
                    objs = new Objects();
                    objs.setDateTime(l.get(0).getDateTime());
                    for(FinalPoint f:l){
                        res.add(f.getUserID());
                    }
                    objs.setObjs(res);
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
    public Map<Long, List<List<FinalPoint>>> merge(Map<Long, List<List<FinalPoint>>> longListMap, Map<Long, List<List<FinalPoint>>> acc1) {
        return null;
    }
}
