package peersim.centralized;

import akka.actor.ActorRef;
import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Network;
import peersim.core.Node;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Comparator.comparing;

/**
 * Created by pratik_k on 4/5/2017.
 */
public class AdaptiveObserver implements Control {
    /**
     *	The protocol to operate on.
     *	@config
     */
    private static final String PAR_PROT="protocol";

    /**
     *	The protocol to operate on.
     *	@config
     */
    private static final String PAR_TRANSPORTPROT="transportprotocol";

    /**
     *	The protocol has adaptive latency or not.
     *	@config
     */
    private static final String PAR_ADAPTIVE_LATENCY="adaptiveLatency";

    /**
     *	The protocol has adaptive bandwidth or not.
     *	@config
     */
    private static final String PAR_ADAPTIVE_BANDWIDTH="adaptiveBandwidth";

    /**
     *	Protocol identifier, obtained from config property
     */
    private final int pid;

    /**
     *	Protocol identifier, obtained from config property
     */
    private final int tpid;

    /**
     *	Adaptive Latency identifier, obtained from config property
     */
    private final boolean adaptiveLatency;

    /**
     *	Adaptive Bandwidth identifier, obtained from config property
     */
    private final boolean adaptiveBandwidth;

    /**
     *	The basic constructor that reads the configuration file.
     *	@param prefix the configuration prefix for this class
     */
    public AdaptiveObserver(String prefix) {
        pid = Configuration.getPid(prefix + "." + PAR_PROT);
        tpid = Configuration.getPid(prefix + "." + PAR_TRANSPORTPROT);
        adaptiveLatency = Configuration.getBoolean(prefix + "." + PAR_ADAPTIVE_LATENCY);
        adaptiveBandwidth = Configuration.getBoolean(prefix + "." + PAR_ADAPTIVE_BANDWIDTH);
    }

    @Override
    public boolean execute() {
        for (int i = 0; i < Network.size(); ++i) {
            Node node = Network.get(i);
            AdaptiveCEP pro = ((AdaptiveCEP)(Network.get(i).getProtocol(pid)));
            //start with the consumer node, consumer node is an operator without any parent
            if(pro.getOperator() && pro.getParent() == null){

                if(adaptiveBandwidth && adaptiveLatency) {
                    NodeParameters nodeParams = measureLatencyBandwidth(node);
                    if(nodeParams.getLatency() > 80 || nodeParams.getBandwidth() < 25) {
                        placeOptimizingLatencyBanwidth(node);
                    }
                    System.out.println( nodeParams.getLatency().intValue() + ";" + nodeParams.getBandwidth().intValue());
                } else if(adaptiveLatency && !adaptiveBandwidth){
                    //latency measured here is end to end from leaf node to root node
                    Double latency = measureLatency(node);
                    //System.out.println(CommonState.getTime() / 60);
                    System.out.println(Math.round(latency));
                    //latency will depend on the number of operators since min and max latency from one node to other node is configured in config file
                    if(latency > 80.0){
                        placeOptimizingLatency(node);
                    }
                } else if(!adaptiveLatency && adaptiveBandwidth) {
                    Double bandwidth = measureBandwidth(node);
                    if(bandwidth < 25.0 && adaptiveBandwidth){
                        placeOptimizingBanwidth(node);
                    }
                    System.out.println( bandwidth.intValue());
                } else {
                    Double latency = measureLatency(node);
                    Double bandwidth = measureBandwidth(node);
                    System.out.println("static: " + latency.intValue() + ";" + bandwidth.intValue());
                }
            }
            //pro.advance();
            if(CommonState.getTime()%8.0 ==0){
                for (int j = 0; j < 3; ++j) {
                    pro.advance();
                }
            }
        }
        return false;
    }

    private void placeOptimizingLatency(Node node) {
        OptimizingHeuristics a = new OptimizingHeuristics("Minimizing", "latency");
        a.placeProducerConsumers(node);
        a.placeIntermediates(node);
        copyNodeProperties(a.placements);
        Double durationA = measureLatency(node);
        LinkedHashMap<Node, Node> swapped = new LinkedHashMap<>();
        for(Map.Entry<Node, Node> entry : a.placements.entrySet()){
            swapped.put(entry.getValue(), entry.getKey());
        }
        copyNodeProperties(swapped);

        OptimizingHeuristicsB b = new OptimizingHeuristicsB("Minimizing", "latency");
        b.allOperators(node);
        b.setPlacements();
        b.placeOperators();
        copyNodeProperties(b.placements);
        Double durationB = measureLatency(node);

        swapped = new LinkedHashMap<>();
        for(Map.Entry<Node, Node> entry : b.placements.entrySet()){
            swapped.put(entry.getValue(), entry.getKey());
        }
        copyNodeProperties(swapped);

        if(durationA < durationB) {
            copyNodeProperties(a.placements);
        } else {
            copyNodeProperties(b.placements);
        }
    }

    private void placeOptimizingBanwidth(Node node) {
        OptimizingHeuristics a = new OptimizingHeuristics("Maximizing", "bandwidth");
        a.placeProducerConsumers(node);
        a.placeIntermediates(node);
        copyNodeProperties(a.placements);

        Double bandwidthA = measureBandwidth(node);
        LinkedHashMap<Node, Node> swapped = new LinkedHashMap<>();
        for(Map.Entry<Node, Node> entry : a.placements.entrySet()){
            swapped.put(entry.getValue(), entry.getKey());
        }

        copyNodeProperties(swapped);
        OptimizingHeuristicsB b = new OptimizingHeuristicsB("Maximizing", "bandwidth");
        b.allOperators(node);
        b.setPlacements();
        b.placeOperators();
        copyNodeProperties(b.placements);
        Double bandwidthB = measureBandwidth(node);

        swapped = new LinkedHashMap<>();
        for(Map.Entry<Node, Node> entry : b.placements.entrySet()){
            swapped.put(entry.getValue(), entry.getKey());
        }
        copyNodeProperties(swapped);

        if(bandwidthA > bandwidthB) {
            copyNodeProperties(a.placements);
        } else {
            copyNodeProperties(b.placements);
        }
    }

    private void placeOptimizingLatencyBanwidth(Node node) {
        OptimizingHeuristics a = new OptimizingHeuristics("Maximizing", "latencybandwidth");
        a.placeProducerConsumers(node);
        a.placeIntermediates(node);
        copyNodeProperties(a.placements);

        NodeParameters latencyBandwidthA = measureLatencyBandwidth(node);
        LinkedHashMap<Node, Node> swapped = new LinkedHashMap<>();
        for(Map.Entry<Node, Node> entry : a.placements.entrySet()){
            swapped.put(entry.getValue(), entry.getKey());
        }

        copyNodeProperties(swapped);
        OptimizingHeuristicsB b = new OptimizingHeuristicsB("Maximizing", "latencybandwidth");
        b.allOperators(node);
        b.setPlacements();
        b.placeOperators();
        copyNodeProperties(b.placements);
        NodeParameters latencyBandwidthB = measureLatencyBandwidth(node);

        swapped = new LinkedHashMap<>();
        for(Map.Entry<Node, Node> entry : b.placements.entrySet()){
            swapped.put(entry.getValue(), entry.getKey());
        }
        copyNodeProperties(swapped);

        if(latencyBandwidthA.compareTo(latencyBandwidthB) > 0) {
            copyNodeProperties(a.placements);
        } else {
            copyNodeProperties(b.placements);
        }
    }

    private void  copyNodeProperties(LinkedHashMap<Node, Node> placements) {
        Map<Long, CloneNodeProperties> cnpMap =  placements.keySet().stream().map(node -> {
            CloneNodeProperties cnp = new CloneNodeProperties();
            AdaptiveCEP nodeProt = ((AdaptiveCEP) (node.getProtocol(pid)));
            cnp.setNodeId(node.getID());
            cnp.setParent(nodeProt.getParent());
            cnp.setDependencies(nodeProt.getDependencies());
            cnp.setOperator(nodeProt.getOperator());
            cnp.setRef(nodeProt.getRef());
            return cnp;
        }).collect(Collectors.toMap(CloneNodeProperties::getNodeId, e -> e));

        Iterator<Map.Entry<Node,Node>> iter = placements.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Node,Node> entry = iter.next();
            Node node = entry.getKey();
            Node selectedNode = entry.getValue();
            AdaptiveCEP selectedNodeProt = ((AdaptiveCEP) (selectedNode.getProtocol(pid)));
            AdaptiveCEP nodeProt = ((AdaptiveCEP) (node.getProtocol(pid)));
            if(node.getID() != selectedNode.getID()) {
                CloneNodeProperties cnp = cnpMap.get(node.getID());
                //copy all properties from node to selected node
                Node currentParent = cnp.getParent();
                Node newParent = placements.get(currentParent);
                //if the parent is changed
                if(currentParent != null && newParent != null &&  newParent.getID() != currentParent.getID()) {
                    selectedNodeProt.setParent(newParent);
                } else {
                    selectedNodeProt.setParent(currentParent);
                }
                List<Node> newDependencies = cnp.getDependencies().stream().map(currentChild -> {
                    Node newChild = placements.get(currentChild);
                    //if the child is changed
                    if(newChild.getID() != currentChild.getID()) {
                        return newChild;
                    } else {
                        return currentChild;
                    }
                }).collect(Collectors.toList());
                selectedNodeProt.setDependencies(newDependencies);
                selectedNodeProt.setRef(cnp.getRef());
                selectedNodeProt.setOperator(true);
                if(!placements.containsValue(node)) {
                    nodeProt.resetNode();
                }
            } else {
                Node currentParent = nodeProt.getParent();
                Node newParent = placements.get(currentParent);
                //if the parent is changed
                if(currentParent != null && newParent != null && newParent.getID() != currentParent.getID()) {
                    selectedNodeProt.setParent(newParent);
                } else {
                    selectedNodeProt.setParent(currentParent);
                }
                List<Node> newDependencies = nodeProt.getDependencies().stream().map(currentChild -> {
                    Node newChild = placements.get(currentChild);
                    //if the child is changed
                    if(newChild.getID() != currentChild.getID()) {
                        return newChild;
                    } else {
                        return currentChild;
                    }
                }).collect(Collectors.toList());
                selectedNodeProt.setDependencies(newDependencies);
            }
        }
    }

    private double latencySelector(Node otherNode, Node dependentOperator) {
        HashMap<Integer, NodeParameters> hostProps = ((AdaptiveCEP)(otherNode.getProtocol(pid))).getHostProps();
        NodeParameters params = hostProps.get((int)dependentOperator.getID());
        return params.getLatency();
        //return ((Transport)otherNode.getProtocol(FastConfig.getTransport(pid))).getLatency(otherNode, dependentOperator);
    }

    private double bandwidthSelector(Node otherNode, Node dependentOperator) {
        HashMap<Integer, NodeParameters> hostProps = ((AdaptiveCEP)(otherNode.getProtocol(pid))).getHostProps();
        NodeParameters params = hostProps.get((int)dependentOperator.getID());
        return params.getBandwidth();
        //return ((NonUniformRandomTransport)otherNode.getProtocol(tpid)).getBandwidth(otherNode, dependentOperator);
    }

    private NodeParameters latencyBandwidthSelector(Node otherNode, Node dependentOperator) {
        HashMap<Integer, NodeParameters> hostProps = ((AdaptiveCEP)(otherNode.getProtocol(pid))).getHostProps();
        return hostProps.get((int)dependentOperator.getID());
        //return ((NonUniformRandomTransport)otherNode.getProtocol(tpid)).getNodeParameters(otherNode, dependentOperator);
    }

    private Double mergeLatency(double d1, double d2){
        return d1 + d2;
    }
    private Double measureLatency(Node node) {
        AdaptiveCEP prot = (AdaptiveCEP)(node.getProtocol(pid));
        if(prot.getDependencies().isEmpty())
            return 0.0;
        else {
            ArrayList<Double> result = prot.getDependencies().stream().map(dependentOperator ->
                    mergeLatency(measureLatency(dependentOperator),
                            latencySelector(node, dependentOperator))
            ).collect(Collectors.toCollection(ArrayList::new));
            return minmax("Minimizing", result);
        }
    }

    private Double mergeBandwidth(double d1, double d2){
        return Math.min(d1, d2);
    }
    private Double measureBandwidth(Node node) {
        AdaptiveCEP prot = (AdaptiveCEP)(node.getProtocol(pid));
        if(prot.getDependencies().isEmpty())
            return Double.MAX_VALUE;
        else {
            ArrayList<Double> result = prot.getDependencies().stream().map(dependentOperator ->
                    mergeBandwidth(measureBandwidth(dependentOperator),
                            bandwidthSelector(node, dependentOperator))
            ).collect(Collectors.toCollection(ArrayList::new));
            return  minmax("Maximizing", result);
        }
    }

    private NodeParameters mergeLatencyBandwidth(NodeParameters d1, NodeParameters d2){
        NodeParameters response = new NodeParameters();
        response.setBandwidth(mergeBandwidth(d1.getBandwidth(), d2.getBandwidth()));
        response.setLatency(mergeLatency(d1.getLatency(), d2.getLatency()));
        return response;
    }
    private NodeParameters measureLatencyBandwidth(Node node) {
        AdaptiveCEP prot = (AdaptiveCEP)(node.getProtocol(pid));
        if(prot.getDependencies().isEmpty())
            return new NodeParameters(0.0, Double.MAX_VALUE);
        else {
            ArrayList<NodeParameters> result = prot.getDependencies().stream().map(dependentOperator ->
                    mergeLatencyBandwidth(measureLatencyBandwidth(dependentOperator),
                            latencyBandwidthSelector(node, dependentOperator))
            ).collect(Collectors.toCollection(ArrayList::new));
            return minmax("Maximizing", result);
        }
    }

    private class OptimizingHeuristics {
        private LinkedHashMap<Node, Node> placements = new LinkedHashMap<>();
        private String optimizing;
        private String selector;

        public OptimizingHeuristics(String optimizing, String selector) {
            this.optimizing = optimizing;
            this.selector = selector;
        }

        public void placeProducerConsumers(Node node){
            AdaptiveCEP nodeProtocol = ((AdaptiveCEP) (node.getProtocol(pid)));
            nodeProtocol.getDependencies().stream().forEach(this::placeProducerConsumers);
            if(nodeProtocol.getOperator() && ( nodeProtocol.getParent() == null || nodeProtocol.getDependencies().isEmpty())){
                placements.put(node, node);
            }
        }

        public void placeIntermediates(Node node) {
            AdaptiveCEP nodeProtocol = ((AdaptiveCEP) (node.getProtocol(pid)));
            nodeProtocol.getDependencies().stream().forEach(this::placeIntermediates);
            if (nodeProtocol.getOperator() && ( nodeProtocol.getParent() != null && !nodeProtocol.getDependencies().isEmpty())) {
                HashMap valuesForHosts = new HashMap<>();
                //check all other nodes that have the minimum latency with the children of the node to be replaced.
                for(int j = 0; j < Network.size(); ++j) {
                    Node otherNode = Network.get(j);
                    AdaptiveCEP otherNodeProt = ((AdaptiveCEP) (otherNode.getProtocol(pid)));
                    ArrayList propValues = null;
                    /*only nodes that are not consumers/producers - If this strategy is used then there may be a case where the same nodes will be repeatedly selected since
                    those are the nodes with most optimum value but still not satisfying the threshold*/
                    List<Long> nodeIds= placements.values().stream().map((key ->
                        key.getID())).collect(Collectors.toCollection(ArrayList::new));
                    if(!nodeIds.contains(otherNode.getID())){
                    //only nodes that are not consumers/producers and do not already have an operator
                    //if(!nodeIds.contains(otherNode.getID()) && !otherNodeProt.getOperator()){
                        switch (selector) {
                            case "latency": {
                                propValues = nodeProtocol.getDependencies().stream().map(dependentOperator ->
                                        latencySelector(otherNode, placements.get(dependentOperator)))
                                        .collect(Collectors.toCollection(ArrayList::new));
                                valuesForHosts.put(otherNode, minmax("Minimizing", propValues));
                                break;
                            }
                            case "bandwidth": {
                                propValues = nodeProtocol.getDependencies().stream().map(dependentOperator ->
                                        bandwidthSelector(otherNode, placements.get(dependentOperator)))
                                        .collect(Collectors.toCollection(ArrayList::new));
                                valuesForHosts.put(otherNode, minmax("Maximizing", propValues));
                                break;
                            }
                            case "latencybandwidth": {
                                propValues = nodeProtocol.getDependencies().stream().map(dependentOperator ->
                                        latencyBandwidthSelector(otherNode, placements.get(dependentOperator)))
                                        .collect(Collectors.toCollection(ArrayList::new));
                                valuesForHosts.put(otherNode, minmax("Maximizing", propValues));
                                break;
                            }
                        }
                    }
                }
                if (valuesForHosts.isEmpty())
                    throw new UnsupportedOperationException("not enough hosts");

                Node selectedNode = minmaxBy(optimizing, valuesForHosts);
                placements.put(node, selectedNode);

                //System.out.println("Selected node "+selectedNode.getID());
            }
        }
    }

    private class OptimizingHeuristicsB {
        private LinkedHashMap<Node, Node> placements = new LinkedHashMap<>();
        private LinkedHashMap<Node, Set<Node>> previousPlacements = new LinkedHashMap<>();
        HashMap<Node, Node> operators = new HashMap<>();
        private String optimizing;
        private String selector;

        public OptimizingHeuristicsB(String optimizing, String selector) {
            this.optimizing = optimizing;
            this.selector = selector;
        }

        public void allOperators(Node node) {
            AdaptiveCEP nodeProtocol = ((AdaptiveCEP) (node.getProtocol(pid)));
            operators.put(node, nodeProtocol.getParent());
            nodeProtocol.getDependencies().stream().forEach(this::allOperators);
        }

        public void setPlacements(){
            operators.forEach((operator, parent) -> {
                placements.put(operator, operator);
                previousPlacements.put(operator, Stream.of(operator).collect(Collectors.toSet()));
            });
        }

        public void placeOperators() {
            List<Boolean> changed = new ArrayList<>();
            operators.forEach((operator, parent) -> {
                if(parent != null) {
                    AdaptiveCEP operatorProtocol = ((AdaptiveCEP) (operator.getProtocol(pid)));
                    if (!operatorProtocol.getDependencies().isEmpty()) {
                        HashMap valuesForHosts = new HashMap<>();
                        Set<Node> allNodes = new HashSet<Node>();
                        for (int j = 0; j < Network.size(); ++j) {
                            Node otherNode = Network.get(j);
                            ArrayList propValues = new ArrayList();
                            if (!placements.values().contains(otherNode) && !(previousPlacements.get(operator).contains(otherNode))) {
                                switch (selector) {
                                    case "latency": {
                                        propValues.add(mergeLatency(minmax(optimizing, operatorProtocol.getDependencies().stream().map(dependentOperator ->
                                                        latencySelector(otherNode, placements.get(dependentOperator)))
                                                        .collect(Collectors.toCollection(ArrayList::new))),
                                                latencySelector(placements.get(parent), otherNode)));
                                        valuesForHosts.put(otherNode, minmax(optimizing, propValues));
                                        break;
                                    }
                                    case "bandwidth": {
                                        propValues.add(mergeBandwidth(minmax(optimizing, operatorProtocol.getDependencies().stream().map(dependentOperator ->
                                                        bandwidthSelector(otherNode, placements.get(dependentOperator)))
                                                        .collect(Collectors.toCollection(ArrayList::new))),
                                                bandwidthSelector(placements.get(parent), otherNode)));
                                        valuesForHosts.put(otherNode, minmax(optimizing, propValues));
                                        break;
                                    }
                                    case "latencybandwidth": {
                                        propValues.add(mergeLatencyBandwidth(minmax(optimizing, operatorProtocol.getDependencies().stream().map(dependentOperator ->
                                                        latencyBandwidthSelector(otherNode, placements.get(dependentOperator)))
                                                        .collect(Collectors.toCollection(ArrayList::new))),
                                                latencyBandwidthSelector(placements.get(parent), otherNode)));
                                        valuesForHosts.put(otherNode, minmax(optimizing, propValues));
                                        break;
                                    }
                                }
                            }
                        }

                        Double currentValue = null;
                        NodeParameters currentLatencyBandwidthValue = null;
                        switch (selector) {
                            case "latency": {
                                currentValue = mergeLatency(minmax(optimizing, operatorProtocol.getDependencies().stream().map(dependentOperator ->
                                                latencySelector(placements.get(operator), placements.get(dependentOperator)))
                                                .collect(Collectors.toCollection(ArrayList::new))),
                                        latencySelector(placements.get(parent), placements.get(operator)));
                                break;
                            }
                            case "bandwidth": {
                                currentValue = mergeBandwidth(minmax(optimizing, operatorProtocol.getDependencies().stream().map(dependentOperator ->
                                                bandwidthSelector(placements.get(operator), placements.get(dependentOperator)))
                                                .collect(Collectors.toCollection(ArrayList::new))),
                                        bandwidthSelector(placements.get(parent), placements.get(operator)));
                                break;
                            }
                            case "latencybandwidth": {
                                currentLatencyBandwidthValue = mergeLatencyBandwidth(minmax(optimizing, operatorProtocol.getDependencies().stream().map(dependentOperator ->
                                                latencyBandwidthSelector(placements.get(operator), placements.get(dependentOperator)))
                                                .collect(Collectors.toCollection(ArrayList::new))),
                                        latencyBandwidthSelector(placements.get(parent), placements.get(operator)));
                                break;
                            }
                        }
                        Boolean noPotentialPlacements = true;
                        if (valuesForHosts.isEmpty()) {
                            allNodes.removeAll(placements.values());
                            allNodes.removeAll(previousPlacements.get(operator));
                            if(allNodes.isEmpty())
                                noPotentialPlacements = true;
                            else
                                throw new UnsupportedOperationException("not enough hosts");
                        }
                        else
                            noPotentialPlacements = false;

                        if (!noPotentialPlacements) {
                            Node selectedNode = minmaxBy(optimizing, valuesForHosts);
                            Boolean changePlacement = null;
                            switch (selector) {
                                case "latency": {
                                    Double value = (Double) valuesForHosts.get(selectedNode);
                                    changePlacement = value < currentValue;
                                    break;
                                }
                                case "bandwidth": {
                                    Double value = (Double) valuesForHosts.get(selectedNode);
                                    changePlacement = value < currentValue;
                                    break;
                                }
                                case "latencybandwidth": {
                                    NodeParameters value = (NodeParameters) valuesForHosts.get(selectedNode);
                                    int i = value.compareTo(currentLatencyBandwidthValue);
                                    if(i<0){
                                        changePlacement = true;
                                    } else {
                                        changePlacement = false;
                                    }
                                    break;
                                }
                            }
                            if (changePlacement) {
                                placements.put(operator, selectedNode);
                                Set replacements = previousPlacements.get(operator);
                                replacements.add(selectedNode);
                                previousPlacements.put(operator, replacements);
                            }
                            changed.add(changePlacement);
                        } else {
                            changed.add(false);
                        }
                    }
                }
            });
            if (changed.contains( true))
                placeOperators();
        }
    }
    private <V extends Comparable<V>> V minmax(String optimizing, List<V> propValues) {
        V value = null;
        switch (optimizing) {
            case "Minimizing":
                value = Collections.max(propValues);
                break;
            case "Maximizing":
                value = Collections.min(propValues);
                break;
        }
        return value;
    }

    private <V extends Comparable<V>> Node minmaxBy(String optimizing, HashMap<Node,V> nodeLatency) {
        Node selectedNode = null;
        switch (optimizing) {
            case "Minimizing":
                selectedNode = Collections.min(nodeLatency.entrySet(), Comparator.comparing(Map.Entry::getValue)).getKey();
                break;
            case "Maximizing":
                selectedNode = Collections.max(nodeLatency.entrySet(), Comparator.comparing(Map.Entry::getValue)).getKey();
                break;
        }
        return selectedNode;
    }

    private class CloneNodeProperties {
        private Long nodeId;
        private Boolean operator = false;
        private Node parent;
        private List<Node> dependencies = new ArrayList<>();
        private ActorRef ref = null;

        public Long getNodeId() {
            return nodeId;
        }

        public void setNodeId(Long nodeId) {
            this.nodeId = nodeId;
        }

        public Boolean getOperator() {
            return operator;
        }

        public void setOperator(Boolean operator) {
            this.operator = operator;
        }

        public Node getParent() {
            return parent;
        }

        public void setParent(Node parent) {
            this.parent = parent;
        }

        public List<Node> getDependencies() {
            return dependencies;
        }

        public void setDependencies(List<Node> dependencies) {
            this.dependencies = dependencies;
        }

        public ActorRef getRef() {
            return ref;
        }

        public void setRef(ActorRef ref) {
            this.ref = ref;
        }
    }
}