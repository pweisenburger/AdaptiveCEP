package peersim.distributed.algorithm1;


import distributedadaptivecep.system.Operator;
import distributedadaptivecep.system.System;
import akka.actor.ActorRef;
import peersim.centralized.NodeParameters;
import peersim.config.Configuration;
import peersim.config.FastConfig;
import peersim.core.*;
import peersim.distributed.ActiveOperator;
import peersim.distributed.TentativeOperator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.ListBuffer;

import java.util.*;

/**
 * Created by pratik_k on 4/6/2017.
 */
public class DistributedNetworkInitializer implements Control {
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
    public DistributedNetworkInitializer(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_PROT);
    }

    private int nodeIndex = 0;
    private List<Node> operatorNodes = new ArrayList<>();
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
        operatorNodes.stream().forEach(this::addTentativeOperators);
        for (int i = 0; i < Network.size(); ++i) {
            AdaptiveCepProtocol pro = ((AdaptiveCepProtocol)(Network.get(i).getProtocol(pid)));
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
    private Set<Long> uniqueNodes = new HashSet<>();
    private void addTentativeOperators(Node n) {
        AdaptiveCepProtocol adv = (AdaptiveCepProtocol)n.getProtocol(pid);
        ActiveOperator activeOperator = adv.getActiveOperator().get();
        if(activeOperator.getParent().isPresent() && !activeOperator.getDependencies().isEmpty()) {
             Linkable linkable =
                    (Linkable) n.getProtocol( FastConfig.getLinkable(pid) );
            if (linkable.degree() > 0) {
                List tentativeOpList = activeOperator.getTentativeOperatorList();
                for(int i =0; i < linkable.degree(); i++) {
                    Node peern = linkable.getNeighbor(i);
                    AdaptiveCepProtocol peernProto = (AdaptiveCepProtocol)peern.getProtocol(pid);
                    if(!peernProto.getTentativeOperator().isPresent() && !peernProto.getActiveOperator().isPresent()) {
                        uniqueNodes.add(peern.getID());
                        TentativeOperator top = new TentativeOperator();
                        top.setActiveOperator(Optional.of(n));
                        //store the reference of the tentaive operator
                        tentativeOpList.add(peern);
                        //set the neighbor node as tentative operator
                        peernProto.setTentativeOperator(Optional.of(top));
                    }
                }
                activeOperator.setTentativeOperatorList(tentativeOpList);
            }
            adv.setActiveOperator(Optional.of(activeOperator));
        }
        //java.lang.System.out.println(uniqueNodes.size());
    }

    //sequentially place Operator/Actorref on the nodes
    private void mapOperatorToNode(Operator op, Node parent) {
        distributedadaptivecep.system.ActiveOperator a = (distributedadaptivecep.system.ActiveOperator) op;
        if (nodeIndex >= Network.size())
            throw new UnsupportedOperationException("not enough hosts");

        Node n = Network.get(nodeIndex);
        AdaptiveCepProtocol adv = (AdaptiveCepProtocol)n.getProtocol(pid);
        ActiveOperator activeOperator = new ActiveOperator();
        activeOperator.setRef(Optional.of(a.actorRef()));
        if(parent != null){
            activeOperator.setParent(Optional.of(parent));
            AdaptiveCepProtocol parentProto = ((AdaptiveCepProtocol)parent.getProtocol(pid));
            ActiveOperator parentActiveOperator = parentProto.getActiveOperator().get();
            List<Node> dep = parentActiveOperator.getDependencies();
            if(dep == null){
                dep = new ArrayList<>();
            }
            dep.add(n);
            parentActiveOperator.setDependencies(dep);
            parentProto.setActiveOperator(Optional.of(parentActiveOperator));
        }
        adv.setActiveOperator(Optional.of(activeOperator));
        nodeIndex++;
        operatorNodes.add(n);
        for (Operator o : JavaConverters.asJavaCollection(op.dependencies())) {
            this.mapOperatorToNode(o, n);
        }
        //JavaConverters.asJavaCollection(op.dependencies()).forEach(this::mapOperatorToNode);
    }
}
