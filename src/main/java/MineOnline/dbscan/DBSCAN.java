package MineOnline.dbscan;

import MineOnline.common.TrajectoryData;

import java.util.*;


//the detail of DBSCAN cluster
public class DBSCAN {

    private static long ID_COUNTER = 0L;//cluster id

    public void setID_COUNTER(long ID_COUNTER) {
        this.ID_COUNTER = ID_COUNTER;
    }

    // point type
    private enum PointStatus {
        NOISE,
        PART_OF_CLUSTER
    }

    private double eps;
    private double minPts;
    private HashMap<String, RawPoint> rawData = new HashMap<>();
    private String dateTime;


    public DBSCAN(double eps, int minPts, String dateTime, List<TrajectoryData> points) {
        this.eps = eps;
        this.minPts = minPts;
        this.dateTime = dateTime;
        for (TrajectoryData p :points){
            if (!rawData.containsKey(p.getPhone())){
                rawData.put(p.getPhone(),new RawPoint(p.getLon(),p.getLat(),p.getGridID(),p.getDateTime()));
            }
        }
    }


    //DBSCAN cluster
    public List<List<FinalPoint>> cluster() {
//	ArrayList<Integer> noisies = new ArrayList<>();
        ArrayList<SimpleCluster> clusters = new ArrayList<>();
        Map<String, PointStatus> visited = new HashMap<>();
        for (String point : rawData.keySet()) {
            if (visited.get(point) != null) {
                continue;
            }
            ArrayList<String> neighbors = getNeighbors(point);
            if (neighbors.size() >= minPts) {
                // DBSCAN does not care about center points
                //String id = SparkEnv.get().executorId();
//        	SimpleCluster cluster = new Cluster(sp);
                SimpleCluster cluster = new SimpleCluster();
                //set the global ID of this cluster
                //cluster.setID(id + "00" + (ID_COUNTER++));
                cluster.setID(ID_COUNTER++);
                clusters.add(expandCluster(cluster, point, neighbors, visited));
            } else {
                visited.put(point, PointStatus.NOISE);
//                noisies.add(point);
            }
        }

        List<List<FinalPoint>> finalPointsList = new ArrayList<>();
        for (SimpleCluster simpleCluster : clusters) {
            List<FinalPoint> finalPoints = new ArrayList<>();
            for (String oid : simpleCluster.getOids()) {
                if(getNeighbors(oid).size() >= minPts){
                    finalPoints.add(new FinalPoint(
                            simpleCluster.getID(), dateTime, oid,true));
                }
                else{
                    finalPoints.add(new FinalPoint(
                            simpleCluster.getID(), dateTime, oid,false));
                }
            }
            finalPointsList.add(finalPoints);

        }


        return finalPointsList;
    }


    // expand cluster
    private SimpleCluster expandCluster(SimpleCluster cluster,
                                        String point,
                                        ArrayList<String> neighbors,
                                        Map<String, PointStatus> visited) {
        cluster.addObject(point);
        visited.put(point, PointStatus.PART_OF_CLUSTER);

        ArrayList<String> seeds = new ArrayList<String>(neighbors);
        int index = 0;
        while (index < seeds.size()) {
            String current = seeds.get(index);
            PointStatus pStatus = visited.get(current);
            // only check non-visited points
            if (pStatus == null) {
                ArrayList<String> currentNeighbors = getNeighbors(current);
                if (currentNeighbors.size() >= minPts) {
                    seeds = merge(seeds, currentNeighbors);
                }
            }

            if (pStatus != PointStatus.PART_OF_CLUSTER) {
                visited.put(current, PointStatus.PART_OF_CLUSTER);
                cluster.addObject(current);
            }
            index++;
        }
        return cluster;
    }


    // merge two lists
    private ArrayList<String> merge(ArrayList<String> one, ArrayList<String> two) {
        HashSet<String> oneSet = new HashSet<String>(one);
        for (String item : two) {
            if (!oneSet.contains(item)) {
                one.add(item);
            }
        }
        return one;
    }


    // return neighbors of a point
    private ArrayList<String> getNeighbors(String point) {
        ArrayList<String> neighbors = new ArrayList<String>();

        for (String neighbor : rawData.keySet()) {//rawData.keySet()//pointsList2
            if (!point.equals(neighbor) && dist(neighbor, point) <= eps) {
                neighbors.add(neighbor);
            }
        }
        return neighbors;
    }



    // compute distance
    private double dist(String neighbor, String point) {
        RawPoint p1 = rawData.get(neighbor);
        RawPoint p2 = rawData.get(point);
        if (p1 == null || p2 == null) {
            return Double.MAX_VALUE;
        }
        double x1 = p1.getLon(), y1 = p1.getLat();
        double x2 = p2.getLon(), y2 = p2.getLat();
        double xdiff = Math.abs(x2 - x1);
        double ydiff = Math.abs(y2 - y1);
        return Math.sqrt(xdiff * xdiff + ydiff * ydiff) ;
    }

}
