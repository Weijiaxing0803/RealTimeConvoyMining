package MineOnline.dbscan;

import MineOnline.common.Cluster;
import MineOnline.common.Objects;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.util.*;

public class GlobalDbscan implements AggregateFunction<
        List<Cluster>,
        List<Cluster>,
        List<Objects>> {

    public double lg;

    public GlobalDbscan(double lg) {
        this.lg = lg;
    }

    @Override
    public List<Cluster> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<Cluster> add(List<Cluster> l, List<Cluster> accumulator) {
        accumulator.addAll(l);
        return accumulator;
    }

    @Override
    public List<Objects> getResult(List<Cluster> accumulator) {
        List<List<FinalPoint>> cluster = new ArrayList<>();

        boolean merge = false;
        boolean innermerge = false;
//        SixteenGrid sixteenGrid = new SixteenGrid(lg);

        List<Objects> result = new ArrayList<>();
        List<String> res;
        Objects objs;

        List<Long> closeIDs;
        List<Cluster> closeCluster;
        List<FinalPoint> intersection;
        long clusterid1,clusterid2;

        List<Cluster> upList = new ArrayList<>();
        List<Cluster> downList = new ArrayList<>();

        for(Cluster o:accumulator){
//            System.out.println(o);
            if(o.isUp()){
                upList.add(o);
            }
            if(o.isDown()){
                downList.add(o);
            }
            if(!o.isUp() && !o.isDown()){
                res = new ArrayList<>();
                for(FinalPoint f:o.getObjs()){
                    res.add(f.getUserID());
                }
                objs = new Objects();
                objs.setObjs(res);
                objs.setDateTime(o.getObjs().get(0).getDateTime());
                objs.setBigPartition(o.getBigPartition());
                result.add(objs);
            }
        }

//        System.out.println(result);
        Collections.sort(upList, Comparator.comparingLong(Cluster::getMinUp));
        Collections.sort(downList, Comparator.comparingLong(Cluster::getMinDown));
        Cluster candidate = null;

        int i = 0,j = 0;
        int i2,j2;
        int usize = upList.size();
        int dsize = downList.size();
        while(i < usize && j < dsize){
            //未合并
            while(!merge && i < usize && j < dsize){
                if(downList.get(j).getBigPartition() < upList.get(i).getBigPartition() - 1){
//                    System.out.println("1");
                    res = new ArrayList<>();
                    for(FinalPoint f:downList.get(j).getObjs()){
                        res.add(f.getUserID());
                    }
                    objs = new Objects();
                    objs.setObjs(res);
                    objs.setDateTime(downList.get(j).getObjs().get(0).getDateTime());
                    objs.setBigPartition(downList.get(j).getBigPartition());
                    result.add(objs);
                    j++;
                    continue;
                }
                else if(upList.get(i).getBigPartition() <= downList.get(j).getBigPartition()){
//                    System.out.println("2");
                    res = new ArrayList<>();
                    for(FinalPoint f:upList.get(i).getObjs()){
                        res.add(f.getUserID());
                    }
                    objs = new Objects();
                    objs.setObjs(res);
                    objs.setDateTime(upList.get(i).getObjs().get(0).getDateTime());
                    objs.setBigPartition(upList.get(i).getBigPartition());
                    result.add(objs);
                    i++;
                    continue;
                }
                //up和down位于相邻分区
                else{
                    if(downList.get(j).getIndex() < upList.get(i).getIndex()){
//                        System.out.println("3");
                        if(downList.get(j).isMerged()){
                            j++;
                            continue;
                        }
                        candidate = downList.get(j);
                        i2 = i;
                        while(i2 < usize && upList.get(i2).getIndex() == upList.get(i).getIndex()){
                            if(upList.get(i2).isMerged()){
                                i2++;
                                continue;
                            }
                            if(isMerge(upList.get(i2).getObjs(),downList.get(j).getObjs())){
                                intersection = Intersection(upList.get(i2).getObjs(),downList.get(j).getObjs());
                                candidate = new Cluster();
                                candidate.setObjs(intersection);
                                merge = true;
                                if(i2 == i){
                                    i++;
                                }
                                else{
                                    upList.get(i2).setMerged(true);
                                }
                                break;
                            }
                            i2++;
                        }
                        if(!merge){
                            res = new ArrayList<>();
                            for(FinalPoint f:downList.get(j).getObjs()){
                                res.add(f.getUserID());
                            }
                            objs = new Objects();
                            objs.setObjs(res);
                            objs.setDateTime(downList.get(j).getObjs().get(0).getDateTime());
                            objs.setBigPartition(downList.get(j).getBigPartition());
                            result.add(objs);
                        }
                        j++;
                    }
                    else{
//                        System.out.println("4");
                        if(upList.get(i).isMerged()){
                            i++;
                            continue;
                        }
                        candidate = upList.get(i);
                        j2 = j;
//                        System.out.println("dj2");
//                        System.out.println(downList.get(j2));
//                        System.out.println("i");
//                        System.out.println(upList.get(i));
                        while(j2 < dsize && downList.get(j2).getIndex() == downList.get(j).getIndex()){
                            if(downList.get(j2).isMerged()){
                                j2++;
                                continue;
                            }
                            if(isMerge(downList.get(j2).getObjs(),upList.get(i).getObjs())){
                                intersection = Intersection(downList.get(j2).getObjs(),upList.get(i).getObjs());
                                candidate = new Cluster();
                                candidate.setObjs(intersection);
                                candidate.setDateTime(downList.get(j2).getDateTime());
                                candidate.setBigPartition(downList.get(j2).getBigPartition());
                                merge = true;
                                if(j2 == j){
                                    j++;
                                }
                                else{
                                    downList.get(j2).setMerged(true);
                                }
                                break;
                            }
                            j2++;
                        }
//                        System.out.println(j);
//                        System.out.println(merge);
                        if(!merge){
                            res = new ArrayList<>();
                            for(FinalPoint f:upList.get(i).getObjs()){
                                res.add(f.getUserID());
                            }
                            objs = new Objects();
                            objs.setObjs(res);
                            objs.setDateTime(upList.get(i).getObjs().get(0).getDateTime());
                            objs.setBigPartition(upList.get(i).getBigPartition());
                            result.add(objs);
                        }
                        i++;
                    }

                }
            }

//            System.out.println("j" + j);
//            System.out.println("dsize" + dsize);
            //已合并
            while(merge && i < usize && j < dsize){
//                System.out.println("5");
                i2 = i;
                while(i2 < usize && upList.get(i2).getIndex() == upList.get(i).getIndex()){
//                    System.out.println("6");
                    if(upList.get(i2).isMerged()){
                        i2++;
                        continue;
                    }
                    if(isMerge(candidate.getObjs(),upList.get(i2).getObjs())){
                        intersection = Intersection(upList.get(i2).getObjs(),candidate.getObjs());
                        candidate.setObjs(intersection);
                        if(i2 == i){
                            i++;
                        }
                        else{
                            upList.get(i2).setMerged(true);
                        }
                        innermerge = true;
                        break;
                    }
                    i2++;
                }
                j2 = j;
                while(j2 < dsize && downList.get(j2).getIndex() == downList.get(j).getIndex()){
//                    System.out.println("7");
                    if(downList.get(j2).isMerged()){
                        j2++;
                        continue;
                    }
                    if(isMerge(candidate.getObjs(),downList.get(j2).getObjs())){
                        intersection = Intersection(downList.get(j2).getObjs(),candidate.getObjs());
                        candidate.setObjs(intersection);
                        if(j2 == j){
                            j++;
                        }
                        else{
                            downList.get(j2).setMerged(true);
                        }
                        innermerge = true;
                        break;
                    }
                    j2++;
                }
                if(!innermerge){
//                    System.out.println("8");
                    res = new ArrayList<>();
                    for(FinalPoint f:candidate.getObjs()){
                        res.add(f.getUserID());
                    }
                    objs = new Objects();
                    objs.setObjs(res);
                    objs.setDateTime(candidate.getObjs().get(0).getDateTime());
                    objs.setBigPartition(candidate.getBigPartition());
                    result.add(objs);
                    merge = false;
                }
                else {
//                    System.out.println("9");
                    innermerge = false;
                }
            }
        }
//        System.out.println(result);
        if(merge){
//            System.out.println("3");
            res = new ArrayList<>();
            for(FinalPoint f:candidate.getObjs()){
                res.add(f.getUserID());
            }
            objs = new Objects();
            objs.setObjs(res);
            objs.setDateTime(candidate.getObjs().get(0).getDateTime());
            objs.setBigPartition(candidate.getBigPartition());
            result.add(objs);
        }
//        System.out.println(result);
        while(i < usize){
//            System.out.println("3");
            res = new ArrayList<>();
            for(FinalPoint f:upList.get(i).getObjs()){
                res.add(f.getUserID());
            }
            objs = new Objects();
            objs.setObjs(res);
            objs.setDateTime(upList.get(i).getObjs().get(0).getDateTime());
            objs.setBigPartition(upList.get(i).getBigPartition());
            result.add(objs);
            i++;
        }
        while (j < dsize){
            res = new ArrayList<>();
            for(FinalPoint f:downList.get(j).getObjs()){
                res.add(f.getUserID());
            }
            objs = new Objects();
            objs.setObjs(res);
            objs.setDateTime(downList.get(j).getObjs().get(0).getDateTime());
            objs.setBigPartition(downList.get(j).getBigPartition());
            result.add(objs);
            j++;
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
    public List<Cluster> merge(List<Cluster> longListMap, List<Cluster> acc1) {
        return null;
    }
}
