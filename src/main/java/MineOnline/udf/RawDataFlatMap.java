package MineOnline.udf;

import MineOnline.common.TrajectoryData;
import MineOnline.grid.SixteenGrid;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

// This class is mainly used to convert a row of input data into TrajectoryData

public class RawDataFlatMap implements FlatMapFunction<String, TrajectoryData> {

    public double lg;

    public RawDataFlatMap(double lg) {
        this.lg = lg;
    }

    @Override
    public void flatMap(String str, Collector<TrajectoryData> out) throws Exception {
        String[] split = str.split(",");
        if (split.length == 5) {
            TrajectoryData trajectoryData = new TrajectoryData();

            double lon = Double.parseDouble(split[0]);//经度，默认0
            double lat = Double.parseDouble(split[1]);//纬度，默认1

            trajectoryData.setPhone(split[2]);
            trajectoryData.setLat(lat);
            trajectoryData.setLon(lon);
            trajectoryData.setDateTime(split[3] + " " + split[4]);

            SixteenGrid sixteenGrid = new SixteenGrid(lg);
            long cellID = sixteenGrid.mapToGridCell(lon, lat);
            if (cellID != -1) {//
                List<Long> closeIDs = sixteenGrid.closeId(cellID);

                //Calculate the grid ID to which it belongs
                trajectoryData.setGridID(cellID);

                for (long closeID : closeIDs) {
                    trajectoryData.setCloseID(closeID);

                    if (closeID == trajectoryData.getGridID()) {
                        trajectoryData.setFlag(false);
                    } else trajectoryData.setFlag(true);

                    out.collect(trajectoryData);
                }

            }

        }

    }
}
