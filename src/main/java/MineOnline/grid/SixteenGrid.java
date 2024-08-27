package MineOnline.grid;

import java.util.ArrayList;
import java.util.List;

//test grid partitioning

public class SixteenGrid {
    private double LON_EAST = 4.0;//用整数(向上) //116.631452;
    private double LON_WEST = 0.0;//向下取整 //116.047967
    private double LAT_NORTH = 4.0;//40.240792
    private double LAT_SOUTH = 0.0;//39.627958

    private double DELTA_LON;

    private long NUMBER_OF_GRID_X;
    private long NUMBER_OF_GRID_Y;

    private int[][] directions = {
            {-1, 0},
            {-1, 1},
            {0, 1},
            {1, 0},
            {1, 1},
            {1, -1},
            {-1, -1},
            {0, -1}
            };

    private int[][] bigDirections = {
            {0, 1},
            {1, -1},
            {1, 0},
            {1, 1},
    };

    public SixteenGrid(double e) {
        this.DELTA_LON = e;
        this.NUMBER_OF_GRID_X = (long) Math.ceil((LON_EAST - LON_WEST) / DELTA_LON);
        this.NUMBER_OF_GRID_Y = (long) Math.ceil((LAT_NORTH - LAT_SOUTH) / DELTA_LON);
    }

    //Specify the cell ID where the point is located
    public long mapToGridCell(double lon, double lat) {

        if (lon>LON_EAST || lon <LON_WEST || lat>LAT_NORTH || lat<LAT_SOUTH){
            return -1;
        }

        // compute the grid of x
        long xIndex = (long) ((Math.abs(lon - LON_WEST)) / DELTA_LON);
        // compute the grid of y
        long yIndex = (long) (Math.abs((LAT_NORTH - lat)) / DELTA_LON);
        return xIndex + (yIndex * NUMBER_OF_GRID_X);
    }


    // compute neighboring grid
    public List<Long> closeId(long id) {
        ArrayList<Long> res = new ArrayList<>();
        long row = id / NUMBER_OF_GRID_X;
        long col = id % NUMBER_OF_GRID_X;
        res.add(id);

        for (int[] dir : directions) {
            long nextRow = row + dir[0];
            long nextCol = col + dir[1];
            if (inArea(nextRow, nextCol)) {
                long newID = nextRow * NUMBER_OF_GRID_X + nextCol;
                res.add(newID);
            }
        }

        return res;
    }

//    compute bigger neighboring grid
    public List<Long> bigCloseId(long id) {
        ArrayList<Long> res = new ArrayList<>();
        long row = id / NUMBER_OF_GRID_X;
        long col = id % NUMBER_OF_GRID_X;

        for (int[] dir : bigDirections) {
            long nextRow = row + dir[0];
            long nextCol = col + dir[1];
            if (inArea(nextRow, nextCol)) {
                long newID = nextRow * NUMBER_OF_GRID_X + nextCol;
                res.add(newID);
            }
        }

        return res;
    }

    private boolean inArea(long row, long col) {
        return row >= 0 && row < NUMBER_OF_GRID_Y && col >= 0 && col < NUMBER_OF_GRID_X;
    }
}
