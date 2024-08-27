package MineOnline.common;

import java.util.ArrayList;
import java.util.List;

//store candidate convoy in previous snapshot
public class LClusterConvoy {

    List<ClusterConvoy> result = new ArrayList<>();

    public LClusterConvoy(List<ClusterConvoy> result) {
        this.result.addAll(result);
    }

    public List<ClusterConvoy> getResult() {
        return result;
    }

    public void setResult(List<ClusterConvoy> result) {
        this.result = result;
    }
}

