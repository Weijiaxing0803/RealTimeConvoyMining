package MineOnline.grid;

import java.util.ArrayList;
import java.util.List;

// 此类用于GeoLife数据集的网格划分

public class GeoLifeGrid {
    // 地理位置范围
    // 最东边的经度
    private double LON_EAST = 117.0;//用整数(向上) //116.9;
    // 最西边的经度
    private double LON_WEST = 115.5;//向下取整 //115.8
    // 最北边的纬度
    private double LAT_NORTH = 40.5;//40.4
    // 最南边的纬度
    private double LAT_SOUTH = 39.5;//39.6

    // 划分网格
    private double DELTA_LON;

    private long NUMBER_OF_GRID_X;
    private long NUMBER_OF_GRID_Y;

    private int[][] directions = {{-1, 0}, {-1, 1}, {-1, -1},
            {0, 1}, {0, -1},
            {1, 0}, {1, -1}, {1, 1}};

    private int[][] bigDirections = {
            {0, 1},
            {1, -1},
            {1, 0},
            {1, 1},
    };

    public GeoLifeGrid(double e) {
        this.DELTA_LON = e;
        this.NUMBER_OF_GRID_X = (int) Math.ceil(Math.abs(LON_EAST - LON_WEST) / DELTA_LON);
        this.NUMBER_OF_GRID_Y = (int) Math.ceil(Math.abs(LAT_NORTH - LAT_SOUTH) / DELTA_LON);
    }

    //指定经纬度所在的单元格 id
    public long mapToGridCell(double lon, double lat) {

        //剔除脏数据
        if (lon>LON_EAST || lon <LON_WEST || lat>LAT_NORTH || lat<LAT_SOUTH){
            return -1;//表示该数据为脏数据
        }

        // 计算 x 坐标方向所在的单元格
        long xIndex = (long) ((Math.abs(lon - LON_WEST)) / DELTA_LON);
        // 计算 y 坐标方向所在的单元格
        long yIndex = (long) (Math.abs((LAT_NORTH - lat)) / DELTA_LON);
        return xIndex + (yIndex * NUMBER_OF_GRID_X);
    }


    // 计算所属网格id的紧邻8个网格id（包括自身网格id）
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


    public long partitionGrid(int node){
        long len = NUMBER_OF_GRID_Y / node;
        return NUMBER_OF_GRID_X * len;
    }

    //closeID是九宫格的Index,node是node节点数量
    public long bigPartition(long closeID, int node){
        long gridnum = partitionGrid(node);
        long res = closeID / gridnum;
        if(node == res){
            res--;
        }
        return res;
    }

    //p是簇所在partition
    public boolean isUp(long closeID,long p,int node){
        if(p == 0){
            return false;
        }
        long gridnum = partitionGrid(node);
        long up = p * gridnum;
        long down = p * gridnum + NUMBER_OF_GRID_X;
        return closeID >= up && closeID < down;
    }

    public boolean isDown(long closeID,long p,int node){
        if(p == node - 1){
            return false;
        }
        long gridnum = partitionGrid(node);
        long up = (p + 1) * gridnum - NUMBER_OF_GRID_X;
        long down = (p + 1) * gridnum;
        return closeID >= up && closeID < down;
    }

    public long indexNum(long smallPartition){
        return smallPartition % NUMBER_OF_GRID_X;
    }



}
