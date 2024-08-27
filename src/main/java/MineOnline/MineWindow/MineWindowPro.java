package MineOnline.MineWindow;

import MineOnline.common.ClusterConvoy;
import MineOnline.common.Convoy;
import MineOnline.common.LClusterConvoy;
import MineOnline.common.Objects;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;



// the detail of RTCM: include SCCP and PCCP
public class MineWindowPro extends ProcessWindowFunction<
        List<Objects>,Convoy,Boolean, TimeWindow
        >{

    ValueState<LClusterConvoy> LCluster;
//    ValueState<StateCount> Counter;

    private int k;
    private long lifespan;
    private int m;

    public MineWindowPro(int k, int m, int timeDis) {
        this.k = k;
        this.lifespan = timeDis * 1000;
        this.m = m;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //      store the candidate convoy
        LCluster = getRuntimeContext().getState(new ValueStateDescriptor<LClusterConvoy>("LCluster", LClusterConvoy.class));
//        Counter = getRuntimeContext().getState(new ValueStateDescriptor<StateCount>("StateCount", StateCount.class));

        super.open(parameters);
    }

    @Override
    public void process(Boolean aBoolean, Context context, Iterable<List<Objects>> iterable, Collector<Convoy> collector) throws Exception {

        List<ClusterConvoy> RightConvoy = new ArrayList<>();
        List<Convoy> Rsubconvoy;

        List<ClusterConvoy> LeftCluster = new ArrayList<>();

        boolean fullMerge = false;

        List<Integer> res;
        List<Integer> subres;

//        Convert the clusters in the current snapshot to primary candidate convoy
        for(List<Objects> l:iterable){
            List<Integer> objs;
            List<String> os;
            for(Objects o:l){
                objs = new ArrayList<>();
                os = o.getObjs();
                for(String oss:os){
                    objs.add(Integer.parseInt(oss));
                }
                ClusterConvoy convoys = new ClusterConvoy(objs,o.getDateTime(),o.getDateTime(),lifespan);
                RightConvoy.add(convoys);
            }
        }

//        if there is no candidate convoy
        if(LCluster.value() == null) {
            LClusterConvoy lc = new LClusterConvoy(RightConvoy);
            LCluster.update(lc);

            return;
        }
        else{
            LeftCluster = LCluster.value().getResult();
        }


//      SCCP
        for(ClusterConvoy Robjs:RightConvoy){
            for(ClusterConvoy Lobjs:LeftCluster){

                //SCCP pruning condition
                if(Lobjs.jump(m)){
                    continue;
                }

                res = SubsequenceBasedIntersections(Lobjs.getObjs(),Robjs.getObjs());
//                count++;

                if(res.isEmpty()){
                    continue;
                }

//                employ a counter for each primary candidate convoy
                Robjs.setCount(res.size() + Robjs.getCount());
                Lobjs.setCount(res.size() + Lobjs.getCount());

                //PCCP
                //intersection greater than m
                // PCCP pruning condidation 1
                if(res.size() >= m){

                    if(res.size() == Robjs.size()){
                        Robjs.setStartTime(Lobjs.getStartTime());
                        fullMerge = true;
                    }
                    for(Convoy LSub:Lobjs.getSubConvoy()){
                        subres = SubsequenceBasedIntersections(LSub.getObjs(),res);
                        if(subres.size() < m){
                            continue;
                        }
                        Convoy convoys = new Convoy(subres,LSub.getStartTime(),Robjs.getEndTime(),lifespan);
                        if(subres.size() == Robjs.size()){
                            if(Robjs.getStartTimeStamp() > LSub.getStartTimeStamp()){
                                Robjs.setStartTime(LSub.getStartTime());
                            }
                        }
                        else {
                            Rsubconvoy = Robjs.getSubConvoy();
                            Rsubconvoy = updateVnext(Rsubconvoy,convoys);
                            Robjs.setSubConvoy(Rsubconvoy);
                        }
                        // PCCP pruning condidation 2
                        if(subres.size() == res.size()){
                            fullMerge = true;
                            break;
                        }
                    }
                    if(!fullMerge){
                        Convoy convoys = new Convoy(res,Lobjs.getStartTime(),Robjs.getEndTime(),lifespan);
                        Rsubconvoy = Robjs.getSubConvoy();
                        Rsubconvoy = updateVnext(Rsubconvoy,convoys);
                        Robjs.setSubConvoy(Rsubconvoy);
                    }

                }
                fullMerge = false;

                //SCCP pruning condition
                if(Robjs.jump(m)){
                    break;
                }
            }
        }


        //output real convoy
        for(ClusterConvoy Robjs:RightConvoy){
            Robjs.setCount(0);
            //sort sub candidate convoy
            Robjs.getSubConvoy().sort(new Comparator<Convoy>() {
                @Override
                public int compare(Convoy o1, Convoy o2) {
                    return o1.size() - o2.size();
                }
            });
            if(Robjs.lifetime() >= k){
                Convoy convoy = new Convoy(Robjs.getObjs(),Robjs.getStartTime(),Robjs.getEndTime(),lifespan);
                collector.collect(convoy);
            }
            for(Convoy convoy:Robjs.getSubConvoy()){
                if(convoy.lifetime() >= k){
                    collector.collect(convoy);
                }
            }
        }

        LClusterConvoy lc = new LClusterConvoy(RightConvoy);
        LCluster.update(lc);
    }


    //Subsequence-Based Intersection
    public static List<Integer> SubsequenceBasedIntersections(List<Integer> objs1, List<Integer> objs2){
        List<Integer> res = new ArrayList<>();
        int num1,num2;
        int sqrtnum1,sqrtnum2;
        int pointer1 = 0;
        int pointer2 = 0;
        int cur1 = 0;
        int cur2 = 0;
        int groupend1;
        int groupend2;
        objs1.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });
        objs2.sort(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1 - o2;
            }
        });

        num1 = objs1.size();
        num2 = objs2.size();
        sqrtnum1 = (int) Math.sqrt(num1);
        sqrtnum2 = (int) Math.sqrt(num2);

        while (cur1 < num1 && cur2 < num2){
            //compare start and last element
            if(cur1 == pointer1 + sqrtnum1){
                pointer1 += sqrtnum1;
            }
            else if(cur2 == pointer2 + sqrtnum2){
                pointer2 += sqrtnum2;
            }

            if(pointer1 + sqrtnum1 - 1 < num1){
                groupend1 = pointer1 + sqrtnum1 - 1;
            }
            else{
                groupend1 = num1 - 1;
            }

            if(objs2.get(cur2) > objs1.get(groupend1)){
                pointer1 += sqrtnum1;
                cur1 = pointer1;
                continue;
            }

            if(pointer2 + sqrtnum2 - 1 < num2){
                groupend2 = pointer2 + sqrtnum2 - 1;
            }
            else{
                groupend2 = num2 - 1;
            }

            if(objs1.get(cur1) > objs2.get(groupend2)){
                pointer2 += sqrtnum2;
                cur2 = pointer2;
                continue;
            }

            //intersection
            while(cur1 <= groupend1 && cur2 <= groupend2){
                if(objs1.get(cur1).equals(objs2.get(cur2))){
                    res.add(objs1.get(cur1));
                    cur1++;
                    cur2++;
                }
                else if(objs1.get(cur1) < objs2.get(cur2)){
                    cur1++;
                }
                else{
                    cur2++;
                }
            }

        }

        return res;
    }

    public List<Convoy> updateVnext(List<Convoy> Vnext, Convoy vnew){
        boolean added = false;
        List<Convoy> result = new ArrayList<>();
        result.addAll(Vnext);
        for(Convoy v: Vnext){
            if(v.hasSameObjs(vnew)){
                if(v.getStartTimeStamp() > vnew.getStartTimeStamp()){//v is a subconvoy of vnew
                    result.remove(v);
                    result.add(vnew);
                    added = true;
                }
                else if(vnew.getStartTimeStamp() > v.getStartTimeStamp()){//vnew is a subconvoy
                    added = true;
                }
            }
        }
        if(!added){
            result.add(vnew);
        }
        return result;
    }

}
