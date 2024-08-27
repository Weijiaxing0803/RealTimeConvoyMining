package MineOnline.udf;

import MineOnline.common.TrajectoryData;
import MineOnline.grid.GeoLifeGrid;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;

// This class is mainly used to convert a row of input data into TrajectoryData for GeoLife dataset

public class RawDataFlatMapGeoLife implements FlatMapFunction<String, TrajectoryData> {

    public double lg;

    public RawDataFlatMapGeoLife(double lg) {
        this.lg = lg;
    }

    @Override
    public void flatMap(String str, Collector<TrajectoryData> out) throws Exception {
        String[] split = str.split(",");
        if (split.length == 5) {
            TrajectoryData trajectoryData = new TrajectoryData();

            double lon = Double.parseDouble(split[0]);
            double lat = Double.parseDouble(split[1]);

            trajectoryData.setPhone(split[2]);
            trajectoryData.setLat(lat);
            trajectoryData.setLon(lon);
            trajectoryData.setDateTime(split[3] + " " + split[4]);

            GeoLifeGrid geoLifeGrid = new GeoLifeGrid(lg);

            long cellID = geoLifeGrid.mapToGridCell(lon, lat);
//            System.out.println(cellID);
            if (cellID != -1) {
                List<Long> closeIDs = geoLifeGrid.closeId(cellID);

                trajectoryData.setGridID(cellID);
//                System.out.println(cellID);
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
