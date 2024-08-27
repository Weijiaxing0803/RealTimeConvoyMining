package MineOnline.ConvoyMinePro;

import MineOnline.MineWindow.MineWindowPro;
import MineOnline.common.Convoy;
import MineOnline.common.Objects;
import MineOnline.common.TrajectoryData;
import MineOnline.dbscan.FinalPoint;
import MineOnline.dbscan.GlobalDbscan;
import MineOnline.dbscan.LocalDbscan;
import MineOnline.udf.RawDataFlatMap;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
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


public class ConvoyMinePro {
    public static void main(String[] args) throws Exception {

//        double eps = 0.5;
//        double lg = 1.0;
//        int m = 3;
//        int k = 3;
//        int minPts = 3;
//        int timeDis = 5; time interval

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


        // 1.Create a stream execution environment
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Set the cycle for generating watermarks
        env.getConfig().setAutoWatermarkInterval(10);


        // 3.input
        DataStreamSource<String> stream1 =
                env.readTextFile(in);


        // 4.Grid division
        SingleOutputStreamOperator<TrajectoryData> stream2 =
                stream1.flatMap(new RawDataFlatMap(eps));


//         watermark
        SingleOutputStreamOperator<TrajectoryData> stream3 = stream2.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TrajectoryData>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<TrajectoryData>() {
                    @Override
                    public long extractTimestamp(TrajectoryData trajectoryData, long l) {
                        return trajectoryData.getTimeStamp();
                    }
                }));

        //*******************************************************************************

        // 5. LocalDbscan
        SingleOutputStreamOperator<Map<Long,List<List<FinalPoint>>>> stream4 = stream3.keyBy(data -> data.closeID)
                .window(TumblingEventTimeWindows.of(Time.seconds(timeDis)))
                .aggregate(new LocalDbscan(eps,minPts))
                .filter(data->(data.values().iterator().next().size() != 0));

        // 6. Global clustering merge
        SingleOutputStreamOperator<List<Objects>> stream5 = stream4.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(timeDis)))
                .aggregate(new GlobalDbscan(eps));

        // 7. convoy generation
        SingleOutputStreamOperator<Convoy> stream6 = stream5.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(timeDis)))
                .process(new MineWindowPro(k,m,timeDis));


        //8. output
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(
                new Path(out),//out path
                new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(10))
                                .build()
                )
                .build();


        DataStreamSink<String> sink = stream6.map(data -> data.toString())
                .addSink(streamingFileSink);
        sink.setParallelism(1);

        env.execute();
    }
}
