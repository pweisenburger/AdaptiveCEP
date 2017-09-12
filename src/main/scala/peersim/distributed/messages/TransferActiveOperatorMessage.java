package peersim.distributed.messages;

import peersim.distributed.ActiveOperator;

/**
 * Created by pratik_k on 7/5/2017.
 */
public class TransferActiveOperatorMessage {
    private ActiveOperator activeOperator;

    public ActiveOperator getActiveOperator() {
        return activeOperator;
    }

    public void setActiveOperator(ActiveOperator activeOperator) {
        this.activeOperator = activeOperator;
    }
}
