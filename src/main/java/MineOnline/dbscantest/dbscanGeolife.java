package MineOnline.dbscantest;

import MineOnline.common.Objects;
import MineOnline.common.TrajectoryData;
import MineOnline.dbscan.FinalPoint;
import MineOnline.dbscan.GlobalDbscanGeolife;
import MineOnline.dbscan.LocalDbscan;
import MineOnline.udf.RawDataFlatMapGeoLife;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class dbscanGeolife {
    public static void main(String[] args) throws Exception {
        // 参数传递
//        double eps = 0.5;
//        double lg = 1.0;
//        int m = 3;
//        int k = 3;
//        int minPts = 3;//之后会固定为 10
//        int timeDis = 5;//单位为秒
        String pathInputFile = "input/16grid4.txt";

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        double eps;
        try{
            eps = parameterTool.getDouble("eps");
        }catch (Exception e){
            System.err.println("need to provide --eps");
            return;
        }
        int m;
        try{
            m = parameterTool.getInt("m");
        }catch (Exception e){
            System.err.println("need to provide  --m");
            return;
        }
        int k;
        try{
            k = parameterTool.getInt("k");
        }catch (Exception e){
            System.err.println("need to provide  --k");
            return;
        }
        int minPts;
        try{
            minPts = parameterTool.getInt("minPts");
        }catch (Exception e){
            System.err.println("need to provide  --minPts");
            return;
        }
        int timeDis;
        try{
            timeDis = parameterTool.getInt("timeDis");
        }catch (Exception e){
            System.err.println("need to provide  --timeDis");
            return;
        }
        String in;
        try{
            in = parameterTool.get("in");
        }catch (Exception e){
            System.err.println("need to provide  --in");
            return;
        }
        String out;
        try{
            out = parameterTool.get("out");
        }catch (Exception e){
            System.err.println("need to provide --out");
            return;
        }

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();



        env.getConfig().setAutoWatermarkInterval(10000);


        DataStreamSource<String> stream1 =
                env.readTextFile(in);
        stream1.setParallelism(1);



        SingleOutputStreamOperator<TrajectoryData> stream2 =
                stream1.flatMap(new RawDataFlatMapGeoLife(eps));


        SingleOutputStreamOperator<TrajectoryData> stream3 = stream2.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TrajectoryData>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<TrajectoryData>() {
                    @Override
                    public long extractTimestamp(TrajectoryData trajectoryData, long l) {
                        return trajectoryData.getTimeStamp();
                    }
                }));

        //*******************************************************************************

        //
        SingleOutputStreamOperator<Map<Long,List<List<FinalPoint>>>> stream4 = stream3.keyBy(data -> data.closeID)
                .window(TumblingEventTimeWindows.of(Time.seconds(timeDis)))
                .aggregate(new LocalDbscan(eps,minPts))
                .filter(data->(data.values().iterator().next().size() != 0));

        SingleOutputStreamOperator<List<Objects>> stream5 = stream4.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(timeDis)))
                .aggregate(new GlobalDbscanGeolife(eps));
//        stream5.print("stream5");

        StreamingFileSink<String> dbscanStream = StreamingFileSink.<String>forRowFormat(
                new Path(out),
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))
                                .build()
                )
                .build();

        DataStreamSink<String> dbscanSink = stream5.map(data -> data.toString())
                .addSink(dbscanStream);


        env.execute();
    }
}
