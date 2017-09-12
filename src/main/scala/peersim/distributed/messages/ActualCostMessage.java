package peersim.distributed.messages;

import peersim.centralized.NodeParameters;
import peersim.core.Node;

/**
 * Created by pratik_k on 6/29/2017.
 */
public class ActualCostMessage {
    private NodeParameters params;
    private Node source;

    public ActualCostMessage(NodeParameters params, Node source) {
        this.params = params;
        this.source = source;
    }

    public NodeParameters getParams() {
        return params;
    }

    public void setParams(NodeParameters params) {
        this.params = params;
    }

    public Node getSource() {
        return source;
    }

    public void setSource(Node source) {
        this.source = source;
    }
}
