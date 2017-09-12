package peersim.centralized;

/**
 * Created by pratik_k on 4/18/2017.
 */
public class NodeParameters implements Comparable<NodeParameters>{
    private Double latency;
    private Double bandwidth;
    private Pair<Double, Integer> progress;

    public NodeParameters(){
        super();
    }
    public NodeParameters(Double latency, Double bandwidth) {
        this.latency = latency;
        this.bandwidth = bandwidth;
    }

    public Pair<Double, Integer> getProgress() {
        return progress;
    }

    public void setProgress(Pair<Double, Integer> progress) {
        this.progress = progress;
    }

    public Double getLatency() {
        return latency;
    }

    public void setLatency(Double latency) {
        this.latency = latency;
    }

    public Double getBandwidth() {
        return bandwidth;
    }

    public void setBandwidth(Double bandwidth) {
        this.bandwidth = bandwidth;
    }

    @Override
    public int compareTo(NodeParameters other) {
        Double d0 = -this.getLatency();
        Double d1 = -other.getLatency();
        Double n0 = this.getBandwidth();
        Double n1 = other.getBandwidth();
        if(d0 == d1 && n0 == n1) return 0;
        else if (d0 < d1 && n0 < n1) return -1;
        else if (d0 > d1 && n0 > n1) return 1;
        else return (int)Math.signum((d0-d1) / Math.abs(d0+d1) + (n0 - n1) / Math.abs(n0 + n1));
    }
}
