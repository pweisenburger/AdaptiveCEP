package peersim.distributed.messages;

import peersim.core.Node;

/**
 * Created by pratik_k on 7/4/2017.
 */
public class StateTransferMessage {
    private Node transferTo;
    private Node source;

    public Node getTransferTo() {
        return transferTo;
    }

    public void setTransferTo(Node transferTo) {
        this.transferTo = transferTo;
    }

    public Node getSource() {
        return source;
    }

    public void setSource(Node source) {
        this.source = source;
    }
}
