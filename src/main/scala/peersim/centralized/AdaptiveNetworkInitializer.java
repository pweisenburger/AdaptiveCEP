package peersim.centralized;


import adaptivecep.system.Operator;
import adaptivecep.system.System;
import akka.actor.ActorRef;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ListBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by pratik_k on 4/6/2017.
 */
public class AdaptiveNetworkInitializer implements Control {
    /**
     *	The protocol to operate on.
     *	@config
     */
    private static final String PAR_PROT="protocol";

    /**
     *	Protocol identifier, obtained from config property
     */
    private final int pid;

    /**
     *	The basic constructor that reads the configuration file.
     *	@param prefix the configuration prefix for this class
     */
    public AdaptiveNetworkInitializer(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    private int nodeIndex = 0;
    /**
     * Performs arbitrary modifications or reports arbitrary information over the
     * components.
     *
     * @return true if the simulation has to be stopped, false otherwise.
     */
    @Override
    public boolean execute() {
        CommonState.r.setSeed(123);
        System sys = new System();
        sys.runQueryInBuilt();
        ListBuffer<ActorRef> actorList = sys.getRoots();
        Seq<Operator> seq = sys.consumers();
        java.lang.System.out.println(seq);

        //JavaConverters.asJavaCollection(seq).forEach(this::mapOperatorToNode);
        for (Operator op : JavaConverters.asJavaCollection(seq)) {
            this.mapOperatorToNode(op, null);
        }
        for (int i = 0; i < Network.size(); ++i) {
            AdaptiveCEP pro = ((AdaptiveCEP)(Network.get(i).getProtocol(pid)));
            if(pro.getOperator() && pro.getParent()!=null){
                java.lang.System.out.println(Network.get(i).getID() + " " + pro.getRef());
                if(pro.getDependencies() != null)
                    pro.getDependencies().stream().forEach(n->java.lang.System.out.println("child " + n.getID()));
                java.lang.System.out.println(Network.get(i).getID() + " parent " + pro.getParent().getID());
            }
            HashMap<Integer, NodeParameters> hostProps = new HashMap<>();
            for (int j = 0; j < Network.size(); ++j) {
                if(i != j) {
                    Node other = Network.get(j);
                    NodeParameters params = new NodeParameters();
                    params.setBandwidth(pro.randomBandwidth());
                    params.setLatency(pro.randomLatency());
                    params.setProgress(null);
                    hostProps.put((int) other.getID(), params);
                }
            }
            pro.setHostProps(hostProps);
        }
        return true;
    }

    //sequentially place Operator/Actorref on the nodes
    private void mapOperatorToNode(Operator op, Node parent) {
        System.ActorOperator a = (System.ActorOperator) op;
        if (nodeIndex >= Network.size())
            throw new UnsupportedOperationException("not enough hosts");

        Node n = Network.get(nodeIndex);
        AdaptiveCEP adv = (AdaptiveCEP)n.getProtocol(pid);
        adv.setRef(a.actorRef());
        adv.setOperator(true);
        if(parent != null){
            adv.setParent(parent);
            AdaptiveCEP parentProto = ((AdaptiveCEP)parent.getProtocol(pid));
            List<Node> dep = parentProto.getDependencies();
            if(dep == null){
                dep = new ArrayList<>();
            }
            dep.add(n);
            parentProto.setDependencies(dep);
        }
        nodeIndex++;
        for (Operator o : JavaConverters.asJavaCollection(op.dependencies())) {
            this.mapOperatorToNode(o, n);
        }
        //JavaConverters.asJavaCollection(op.dependencies()).forEach(this::mapOperatorToNode);
    }
}
