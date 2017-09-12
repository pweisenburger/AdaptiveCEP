package peersim.centralized;

import akka.actor.ActorRef;
import peersim.cdsim.CDProtocol;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by pratik_k on 4/5/2017.
 */
public class AdaptiveCEP implements CDProtocol {

    private Boolean operator = false;
    private Node parent;
    private List<Node> dependencies = new ArrayList<>();
    private ActorRef ref = null;
    private HashMap<Integer, NodeParameters> hostProps = new HashMap<>();

    public HashMap<Integer, NodeParameters> getHostProps() {
        return hostProps;
    }

    public void setHostProps(HashMap<Integer, NodeParameters> hostProps) {
        this.hostProps = hostProps;
    }

    public ActorRef getRef() {
        return ref;
    }

    public void setRef(ActorRef ref) {
        this.ref = ref;
    }

    public Boolean getOperator() {
        return operator;
    }

    public void setOperator(Boolean operator) {
        this.operator = operator;
    }

    public List<Node> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<Node> dependencies) {
        this.dependencies = dependencies;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node parent) {
        this.parent = parent;
    }

    public AdaptiveCEP(String prefix){
        super();
    }

    public Object clone() {
        Object prot = null;
        try {
            prot =  super.clone();
        } catch (Exception e) {
        }
        ((AdaptiveCEP) prot).dependencies = new ArrayList<>();
        ((AdaptiveCEP) prot).ref = null;
        ((AdaptiveCEP) prot).operator = false;
        return prot;
    }

    public void resetNode(){
        this.setOperator(false);
        this.setParent(null);
        this.setDependencies(new ArrayList<>());
        this.setRef(null);
    }

    public double randomLatency()
    {
        return (2 + 98 * CommonState.r.nextDouble());
    }

    public double randomBandwidth()
    {
        return (5 + 95 * CommonState.r.nextDouble());
    }

    public void advance() {
        HashMap<Integer, NodeParameters> hostProps = this.getHostProps();
        for (int j = 0; j < Network.size(); ++j) {
            Node other = Network.get(j);
            NodeParameters params = hostProps.get((int)other.getID());
            if(params != null) {
                Pair<Double, Integer> progress = params.getProgress();
                if(progress != null){
                    Double stepAmount = progress.getLeft();
                    Integer stepCount = progress.getRight();
                    Double bandwidth = params.getBandwidth();
                    Double newBandwidth = bandwidth + stepAmount;
                    Double latency = params.getLatency();
                    Double newLatency = latency + stepAmount;
                    Integer newStepCount = stepCount - 1;
                    if (newLatency < 2 || newLatency > 100 || newBandwidth < 5 || newBandwidth > 100 || newStepCount < 0)
                        progress = null;
                    else {
                        params.setBandwidth(newBandwidth);
                        params.setLatency(newLatency);
                        progress.setLeft(stepAmount);
                        progress.setRight(newStepCount);
                    }
                    params.setProgress(progress);
                } else {
                    progress = nextStepAmountCount();
                    params.setProgress(progress);
                }
            }
            hostProps.put((int)other.getID(), params);
        }
        this.setHostProps(hostProps);
    }

    private Pair<Double, Integer> nextStepAmountCount() {
        return new Pair<>(0.4 - 0.8 * CommonState.r.nextDouble(), 1 + CommonState.r.nextInt(900));
    }

    @Override
    public void nextCycle(Node node, int i) {

    }
}
