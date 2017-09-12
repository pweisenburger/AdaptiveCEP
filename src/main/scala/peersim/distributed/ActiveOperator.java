package peersim.distributed;

import akka.actor.ActorRef;
import peersim.centralized.NodeParameters;
import peersim.core.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

/**
 * Created by pratik_k on 6/27/2017.
 */
public class ActiveOperator {
    private Optional<Node> parent = Optional.empty();
    private List<Node> dependencies = new ArrayList<>();
    private Optional<ActorRef> ref = Optional.empty();
    private List<Node> tentativeOperatorList = new ArrayList<>();
    private HashMap<Node, HashMap<Node, NodeParameters>> nodeNetworkValues = new HashMap<>();
    private HashMap<Node, HashMap<Node, NodeParameters>> optimumPath = new HashMap<>();
    private HashMap<Node, NodeParameters> childrenNodeParameters = new HashMap<>();
    private HashMap<Node, HashMap<Node, NodeParameters>> previousOptimumPath = new HashMap<>();

    public HashMap<Node, HashMap<Node, NodeParameters>> getPreviousOptimumPath() {
        return previousOptimumPath;
    }

    public void setPreviousOptimumPath(HashMap<Node, HashMap<Node, NodeParameters>> previousOptimumPath) {
        this.previousOptimumPath = previousOptimumPath;
    }


    public HashMap<Node, NodeParameters> getChildrenNodeParameters() {
        return childrenNodeParameters;
    }

    public void setChildrenNodeParameters(HashMap<Node, NodeParameters> childrenNodeParameters) {
        this.childrenNodeParameters = childrenNodeParameters;
    }

    public Optional<Node> getParent() {
        return parent;
    }

    public void setParent(Optional<Node> parent) {
        this.parent = parent;
    }

    public List<Node> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<Node> dependencies) {
        this.dependencies = dependencies;
    }

    public Optional<ActorRef> getRef() {
        return ref;
    }

    public void setRef(Optional<ActorRef> ref) {
        this.ref = ref;
    }

    public HashMap<Node, HashMap<Node, NodeParameters>> getNodeNetworkValues() {
        return nodeNetworkValues;
    }

    public void setNodeNetworkValues(HashMap<Node, HashMap<Node, NodeParameters>> nodeNetworkValues) {
        this.nodeNetworkValues = nodeNetworkValues;
    }

    public List<Node> getTentativeOperatorList() {
        return tentativeOperatorList;
    }

    public void setTentativeOperatorList(List<Node> tentativeOperatorList) {
        this.tentativeOperatorList = tentativeOperatorList;
    }

    public HashMap<Node, HashMap<Node, NodeParameters>> getOptimumPath() {
        return optimumPath;
    }

    public void setOptimumPath(HashMap<Node, HashMap<Node, NodeParameters>> optimumPath) {
        this.optimumPath = optimumPath;
    }

    public ActiveOperator clone() {
        ActiveOperator newOp = new ActiveOperator();
        newOp.setParent(this.parent);
        newOp.setRef(this.ref);
        newOp.setDependencies(this.dependencies);
        return newOp;
    }
}
