package peersim.distributed;

import peersim.centralized.NodeParameters;
import peersim.core.Node;

import java.util.HashMap;
import java.util.Optional;

/**
 * Created by pratik_k on 6/27/2017.
 */
public class TentativeOperator {
    private Optional<Node> activeOperator = Optional.empty();
    private HashMap<Node, HashMap<Node, NodeParameters>> nodeNetworkValues = new HashMap<>();
    private HashMap<Node, HashMap<Node, NodeParameters>> optimumPath = new HashMap<>();
    private HashMap<Node, HashMap<Node, NodeParameters>> previousOptimumPath = new HashMap<>();

    public HashMap<Node, HashMap<Node, NodeParameters>> getPreviousOptimumPath() {
        return previousOptimumPath;
    }

    public void setPreviousOptimumPath(HashMap<Node, HashMap<Node, NodeParameters>> previousOptimumPath) {
        this.previousOptimumPath = previousOptimumPath;
    }

    public Optional<Node> getActiveOperator() {
        return activeOperator;
    }

    public void setActiveOperator(Optional<Node> activeOperator) {
        this.activeOperator = activeOperator;
    }

    public HashMap<Node, HashMap<Node, NodeParameters>> getNodeNetworkValues() {
        return nodeNetworkValues;
    }

    public void setNodeNetworkValues(HashMap<Node, HashMap<Node, NodeParameters>> nodeNetworkValues) {
        this.nodeNetworkValues = nodeNetworkValues;
    }

    public HashMap<Node, HashMap<Node, NodeParameters>> getOptimumPath() {
        return optimumPath;
    }

    public void setOptimumPath(HashMap<Node, HashMap<Node, NodeParameters>> optimumPath) {
        this.optimumPath = optimumPath;
    }
}
