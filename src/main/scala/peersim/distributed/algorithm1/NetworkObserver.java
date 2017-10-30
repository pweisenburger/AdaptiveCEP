package peersim.distributed.algorithm1;

import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.Control;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.distributed.ActiveOperator;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by pratik_k on 7/11/2017.
 */
public class NetworkObserver implements Control {
    /**
     *	The protocol to operate on.
     *	@config
     */
    private static final String PAR_PROT="protocol";

    /**
     *	Protocol identifier, obtained from config property
     */
    private final int pid;

    public NetworkObserver(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    @Override
    public boolean execute() {
        Set<Long> uniqueNodes = new HashSet<>();
        for (int i = 0; i < Network.size(); ++i) {
            AdaptiveCepProtocol pro = ((AdaptiveCepProtocol) (Network.get(i).getProtocol(pid)));
            if(pro.getActiveOperator().isPresent()) {
                ActiveOperator activeOperator = pro.getActiveOperator().get();
                if(activeOperator.getParent().isPresent() && !activeOperator.getDependencies().isEmpty()) {
                    Linkable linkable =
                            (Linkable) Network.get(i).getProtocol( FastConfig.getLinkable(pid) );
                    if (linkable.degree() > 0) {
                        for(int j =0; j < linkable.degree(); j++) {
                            Node peern = linkable.getNeighbor(j);
                            AdaptiveCepProtocol peernProto = (AdaptiveCepProtocol)peern.getProtocol(pid);
                            if(peernProto.getTentativeOperator().isPresent() && !peernProto.getActiveOperator().isPresent()) {
                                uniqueNodes.add(peern.getID());
                            }
                        }
                    }
                }
            }
        }
        System.out.println("total number of tentative operators: " + uniqueNodes.size());
        return false;
    }
}
