package peersim.distributed.algorithm1;

import peersim.cdsim.CDProtocol;
import peersim.centralized.NodeParameters;
import peersim.centralized.Pair;
import peersim.config.FastConfig;
import peersim.core.CommonState;
import peersim.core.Linkable;
import peersim.core.Network;
import peersim.core.Node;
import peersim.distributed.ActiveOperator;
import peersim.distributed.TentativeOperator;
import peersim.distributed.messages.ActualCostMessage;
import peersim.distributed.messages.OptimumCostMessage;
import peersim.distributed.messages.StateTransferMessage;
import peersim.distributed.messages.TransferActiveOperatorMessage;
import peersim.edsim.EDProtocol;
import peersim.transport.Transport;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by pratik_k on 6/27/2017.
 */
public class AdaptiveCepProtocol extends StaticValuesHolder implements EDProtocol, CDProtocol {

    public AdaptiveCepProtocol(String prefix){
        super(prefix);
    }
    private Optional<ActiveOperator> activeOperator = Optional.empty();
    private Optional<TentativeOperator> tentativeOperator = Optional.empty();
    private HashMap<Integer, NodeParameters> hostProps = new HashMap<>();

    HashMap<Integer, NodeParameters> getHostProps() {
        return hostProps;
    }

    void setHostProps(HashMap<Integer, NodeParameters> hostProps) {
        this.hostProps = hostProps;
    }

    Optional<ActiveOperator> getActiveOperator() {
        return activeOperator;
    }

    void setActiveOperator(Optional<ActiveOperator> activeOperator) {
        this.activeOperator = activeOperator;
    }

    Optional<TentativeOperator> getTentativeOperator() {
        return tentativeOperator;
    }

    void setTentativeOperator(Optional<TentativeOperator> tentativeOperator) {
        this.tentativeOperator = tentativeOperator;
    }

    public Object clone() {
        Object prot = null;
        try {
            prot =  super.clone();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return prot;
    }

    @Override
    public void nextCycle(Node node, int pid) {
        AdaptiveCepProtocol cepProtocol = (AdaptiveCepProtocol) node.getProtocol(pid);
        Optional<ActiveOperator> activeOperatorOptional = cepProtocol.getActiveOperator();
        if(activeOperatorOptional.isPresent()) {
            ActiveOperator activeOperator = activeOperatorOptional.get();
            if(activeOperator.getDependencies().isEmpty()) {
                Optional<Node> optionalParent = activeOperator.getParent();
                if(!optionalParent.isPresent()) {
                    try {
                        throw new Exception("Source node with no parent");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    Node parent = optionalParent.get();
                    Transport transportProtocol = ((Transport) node.getProtocol(FastConfig.getTransport(pid)));
                    transportProtocol.send(node, parent, new ActualCostMessage(hostProps.get((int)parent.getID()),node), pid);
                    transportProtocol.send(node, parent, new OptimumCostMessage(hostProps.get((int)parent.getID()), node), pid);
                    AdaptiveCepProtocol parentProto = (AdaptiveCepProtocol)parent.getProtocol(pid);
                    List<Node> tentativeParentNodes = parentProto.getActiveOperator().get().getTentativeOperatorList();
                    if(!tentativeParentNodes.isEmpty()) {
                        tentativeParentNodes.forEach(tentativeParentNode ->
                                transportProtocol.send(node, tentativeParentNode,
                                        new OptimumCostMessage(hostProps.get((int)tentativeParentNode.getID()), node), pid));
                    }
                }
            }
        }
        for (int j = 0; j < 2; ++j) {
            cepProtocol.advance();
        }
    }

    @Override
    public void processEvent(Node node, int pid, Object o) {
        AdaptiveCepProtocol cepProtocol = (AdaptiveCepProtocol) node.getProtocol(pid);
        Transport transportProtocol = ((Transport) node.getProtocol(FastConfig.getTransport(pid)));
        Optional<ActiveOperator> activeOperatorOptional = cepProtocol.getActiveOperator();
        Optional<TentativeOperator> tentativeOperatorOptional = cepProtocol.getTentativeOperator();
        if(o instanceof ActualCostMessage) {
            if(activeOperatorOptional.isPresent()) {
                ActualCostMessage actualCostMessage = (ActualCostMessage)o;
                Node source = actualCostMessage.getSource();
                ActiveOperator activeOperator = activeOperatorOptional.get();
                if(activeOperator.getDependencies().contains(source)) {
                    NodeParameters params = actualCostMessage.getParams();
                    HashMap<Node, NodeParameters> childrenNodeParameters = activeOperator.getChildrenNodeParameters();
                    childrenNodeParameters = (HashMap<Node, NodeParameters>)childrenNodeParameters.entrySet().stream()
                            .filter(e -> activeOperator.getDependencies().contains(e.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    childrenNodeParameters.put(source, params);
                    activeOperator.setChildrenNodeParameters(childrenNodeParameters);
                    /*send the actual cost message to the parent when cost message from all dependencies is received*/
                    if(childrenNodeParameters.size() == activeOperator.getDependencies().size()) {
                        Optional<Node> optionalParent = activeOperator.getParent();
                        if(optionalParent.isPresent()) {
                            Node parent = optionalParent.get();
                            NodeParameters summedCost = new NodeParameters();
                            NodeParameters actualValue = new NodeParameters();
                            if(cepProtocol.isAdaptiveLatency() && cepProtocol.isAdaptiveBandwidth()) {
                                actualValue = minmax("Maximizing", new ArrayList<>(childrenNodeParameters.values()));
                                summedCost = mergeLatencyBandwidth(actualValue ,hostProps.get((int)parent.getID()));
                            } else if(cepProtocol.isAdaptiveLatency()) {
                                List<Double> latencies = childrenNodeParameters.values().stream()
                                        .map(NodeParameters::getLatency).collect(Collectors.toList());
                                Double optimumLatency = minmax("Minimizing", latencies);
                                actualValue.setLatency(optimumLatency);
                                summedCost.setLatency(mergeLatency(actualValue ,hostProps.get((int)parent.getID())));
                            } else {
                                List<Double> bandwidth = childrenNodeParameters.values().stream()
                                        .map(NodeParameters::getBandwidth).collect(Collectors.toList());
                                Double optimumBandwidth = minmax("Maximizing", bandwidth);
                                actualValue.setBandwidth(optimumBandwidth);
                                summedCost.setBandwidth(mergeBandwidth(actualValue ,hostProps.get((int)parent.getID())));
                            }
                            transportProtocol.send(node, parent, new ActualCostMessage(summedCost,node), pid);
                        }
                    }
                } else {
                    System.err.println("ActualCostMessage: This message shall be received only by dependent/children Active Operator");
                }
            } else {
                System.err.println("ActualCostMessage: This message cannot be sent by any other node except Active Operator");
            }
        } else if(o instanceof OptimumCostMessage) {
            OptimumCostMessage optimumCostMessage = (OptimumCostMessage)o;
            Node messageSource = optimumCostMessage.getSource();
            /*check if node has an active operator*/
            if(activeOperatorOptional.isPresent()) {
            /*active operator that has received the message*/
                ActiveOperator activeOperator = activeOperatorOptional.get();
            /*Get the source node from where the message is received*/
                HashMap<Node, HashMap<Node, NodeParameters>> nodeParamValues = activeOperator.getNodeNetworkValues();
            /*stores the node identifier from where the message is received and the cost message*/
                nodeParamValues = this.getUpdatedNodeParamValues(nodeParamValues, optimumCostMessage, messageSource, pid);

            /*set the map which contains nodes and its cost messages*/
                activeOperator.setNodeNetworkValues(nodeParamValues);
            /*check if messages has been received from direction of all the dependencies*/
                if(nodeParamValues.size() == activeOperator.getDependencies().size()) {
                    HashMap<Node, Integer> tentativeOperatorOfChildren = new HashMap<>();
                /*store in a map the number of cost messages that are expected before the operator will send the optimum value among them.
                * Number of cost messages = number of tentative operators of the child + the child
                **/
                    nodeParamValues.keySet().stream().forEach(child -> {
                        AdaptiveCepProtocol childProtocol = (AdaptiveCepProtocol) child.getProtocol(pid);
                        // + 1 is for the child
                        tentativeOperatorOfChildren.put(child, childProtocol.getActiveOperator().get().getTentativeOperatorList().size()+1);
                    });

                    HashMap<Node, HashMap<Node, NodeParameters>> optimumPath = activeOperator.getOptimumPath();
                /*find the optimized value from each dependency tree direction*/
                    optimumPath = this.findOptimizedPath(nodeParamValues, tentativeOperatorOfChildren, cepProtocol, optimumPath);

                /*Send a optimum value only when an optimum value is received from direction of all branches/dependencies */
                    if(optimumPath.size() == nodeParamValues.size()) {
                    /*find the optimum value among the optimum node from the branches*/
                        NodeParameters optimumValue = this.findOptimumValue(optimumPath, cepProtocol);

                        if(!activeOperator.getDependencies().isEmpty()) {
                            Optional<Node> optionalParent = activeOperator.getParent();
                            if(!optionalParent.isPresent()) {
                                List<Node> dependencies = activeOperator.getDependencies();
                                /*HashMap<Node, HashMap<Node, NodeParameters>> finalNodeParamValues = nodeParamValues;
                                List<NodeParameters> dependencyValue = dependencies.stream().map(dependency -> {
                                    HashMap<Node, NodeParameters> allValues = finalNodeParamValues.get(dependency);
                                    return allValues.get(dependency);
                                }).collect(Collectors.toList());*/

                                if(cepProtocol.isAdaptiveLatency() && cepProtocol.isAdaptiveBandwidth()) {
                                    List<NodeParameters> dependencyValue = new ArrayList<>(activeOperator.getChildrenNodeParameters().values());
                                    if(dependencyValue.size() != 0) {
                                        NodeParameters currentValue = minmax("Maximizing", dependencyValue);
                                        //System.out.println("Current Cost "+ currentValue.getLatency() + " " + currentValue.getBandwidth() + " Optimum cost " + optimumValue);
                                        System.out.println(currentValue.getLatency().intValue() + "," + currentValue.getBandwidth().intValue());
                                        /*Ask the child node to adapt only if threshold is breached
                                         **/
                                        if ((currentValue.getBandwidth() < 25.00 && optimumValue.getBandwidth() < currentValue.getBandwidth()) || (currentValue.getLatency() > 80.00 && optimumValue.getLatency() < currentValue.getLatency())) {
                                            HashMap<Node, HashMap<Node, NodeParameters>> finalOptimumPath = optimumPath;
                                            List<Node> newDependencies = dependencies.stream().map(dependency -> {
                                                HashMap<Node, NodeParameters> optimumNodeMap = finalOptimumPath.get(dependency);
                                                this.sendStateTransferMessage(optimumNodeMap, node, dependency, transportProtocol, pid);
                                                return optimumNodeMap.keySet();
                                            }).collect(Collectors.toList()).stream().flatMap(Set::stream)
                                                    .collect(Collectors.toList());
                                        /*Change the current dependencies to the new dependencies i.e. the node with optimum value*/
                                            activeOperator.setDependencies(newDependencies);
                                        }
                                    }
                                } else if(cepProtocol.isAdaptiveLatency()) {
                                    List<Double> latencies = activeOperator.getChildrenNodeParameters().values().stream()
                                            .map(NodeParameters::getLatency).collect(Collectors.toList());
                                    /*for cases when actual cost message is not yet received*/
                                    if(latencies.size() != 0) {
                                        Double currentLatencyValue = minmax("Minimizing", latencies);
                                        //System.out.println("Current Cost "+ currentLatencyValue +" Optimum cost " + optimumValue.getLatency());
                                        System.out.println(currentLatencyValue.intValue());
                                        /*Ask the child node to adapt only if threshold is breached and the optimum value from other tentative
                                         * operator is less than the current value
                                         **/
                                        if(currentLatencyValue > 80.00 && optimumValue.getLatency() < currentLatencyValue) {
                                            HashMap<Node, HashMap<Node, NodeParameters>> finalOptimumPath = optimumPath;
                                            List<Node> newDependencies = dependencies.stream().map(dependency -> {
                                                HashMap<Node, NodeParameters> optimumNodeMap = finalOptimumPath.get(dependency);
                                                this.sendStateTransferMessage(optimumNodeMap, node, dependency, transportProtocol, pid);
                                                return optimumNodeMap.keySet();
                                            }).collect(Collectors.toList()).stream().flatMap(Set::stream)
                                                    .collect(Collectors.toList());
                                        /*Change the current dependencies to the new dependencies i.e. the node with optimum value*/
                                            activeOperator.setDependencies(newDependencies);
                                        }
                                    }
                                } else {
                                    List<Double> bandwidth = activeOperator.getChildrenNodeParameters().values().stream()
                                            .map(NodeParameters::getBandwidth).collect(Collectors.toList());
                                    if(bandwidth.size() != 0) {
                                        Double currentBandwidthValue = minmax("Maximizing", bandwidth);
                                        //System.out.println("Current Cost "+ currentBandwidthValue +" Optimum cost " + optimumValue.getBandwidth());
                                        System.out.println(currentBandwidthValue.intValue());
                                        /*Ask the child node to adapt only if threshold is breached and the optimum value from other tentative
                                         * operator is less than the current value
                                         **/
                                        if (currentBandwidthValue < 25.00 && optimumValue.getBandwidth() > currentBandwidthValue) {
                                            HashMap<Node, HashMap<Node, NodeParameters>> finalOptimumPath = optimumPath;
                                            List<Node> newDependencies = dependencies.stream().map(dependency -> {
                                                HashMap<Node, NodeParameters> optimumNodeMap = finalOptimumPath.get(dependency);
                                                this.sendStateTransferMessage(optimumNodeMap, node, dependency, transportProtocol, pid);
                                                return optimumNodeMap.keySet();
                                            }).collect(Collectors.toList()).stream().flatMap(Set::stream)
                                                    .collect(Collectors.toList());
                                        /*Change the current dependencies to the new dependencies i.e. the node with optimum value*/
                                            activeOperator.setDependencies(newDependencies);
                                        }
                                    }
                                }
                            } else {
                                Node parent = optionalParent.get();
                                this.sendMessage(optimumValue, node, parent, cepProtocol, pid);

                                AdaptiveCepProtocol parentsNodeProtocol = (AdaptiveCepProtocol) parent.getProtocol(pid);

                                List<Node> tentativeParentNodes = parentsNodeProtocol.getActiveOperator().get().getTentativeOperatorList();
                                if(!tentativeParentNodes.isEmpty()) {
                                    tentativeParentNodes.forEach(tentativeParentNode -> {
                                        this.sendMessage(optimumValue, node, tentativeParentNode, parentsNodeProtocol, pid);
                                    });
                                }
                            }
                            activeOperator.setPreviousOptimumPath(optimumPath);
                            activeOperator.setOptimumPath(new HashMap<>());
                            activeOperator.setNodeNetworkValues(new HashMap<>());
                        }
                    }
                }
            } else if(tentativeOperatorOptional.isPresent()){
                TentativeOperator tentativeOperator = tentativeOperatorOptional.get();
                Node neighborActiveOperatorNode = tentativeOperator.getActiveOperator().get();
                AdaptiveCepProtocol neighborActiveOperatorNodeProtocol = (AdaptiveCepProtocol) neighborActiveOperatorNode.getProtocol(pid);
                ActiveOperator neighborActiveOperator = neighborActiveOperatorNodeProtocol.getActiveOperator().get();

            /*Get the source node from where the message is received*/
                HashMap<Node, HashMap<Node, NodeParameters>> nodeParamValues = tentativeOperator.getNodeNetworkValues();
                nodeParamValues = this.getUpdatedNodeParamValues(nodeParamValues, optimumCostMessage, messageSource, pid);

            /*set the map which contains nodes and its cost messages*/
                tentativeOperator.setNodeNetworkValues(nodeParamValues);
            /*check if messages has been received from direction of all the dependencies*/
                if(nodeParamValues.size() == neighborActiveOperator.getDependencies().size()) {
                    HashMap<Node, Integer> tentativeOperatorOfChildren = new HashMap<>();
                /*store in a map the number of cost messages that are expected before the operator will send the optimum value among them.
                * Number of cost messages = number of tentative operators of the child + the child
                **/
                    nodeParamValues.keySet().stream().forEach(child -> {
                        AdaptiveCepProtocol childProtocol = (AdaptiveCepProtocol) child.getProtocol(pid);
                        // + 1 is for the child
                        tentativeOperatorOfChildren.put(child, childProtocol.getActiveOperator().get().getTentativeOperatorList().size() + 1);
                    });

                    HashMap<Node, HashMap<Node, NodeParameters>> optimumPath = tentativeOperator.getOptimumPath();
                /*find the optimized value from each dependency tree direction*/
                    optimumPath = this.findOptimizedPath(nodeParamValues, tentativeOperatorOfChildren, cepProtocol, optimumPath);

                /*Send a optimum value only when an optimum value is received from direction of all branches/dependencies */
                    if(optimumPath.size() == nodeParamValues.size()) {
                    /*find the optimum value among the optimum node from the branches*/
                        NodeParameters optimumValue = this.findOptimumValue(optimumPath, cepProtocol);

                        Optional<Node> optionalParent = neighborActiveOperator.getParent();
                        Node neighborsParent = optionalParent.get();
                        this.sendMessage(optimumValue, node, neighborsParent, cepProtocol, pid);

                        AdaptiveCepProtocol neighborsParentsNodeProtocol = (AdaptiveCepProtocol) neighborsParent.getProtocol(pid);

                        List<Node> tentativeParentNodes = neighborsParentsNodeProtocol.getActiveOperator().get().getTentativeOperatorList();
                        if (!tentativeParentNodes.isEmpty()) {
                            tentativeParentNodes.forEach(tentativeParentNode -> {
                                this.sendMessage(optimumValue, node, tentativeParentNode, cepProtocol, pid);
                            });
                        }
                        tentativeOperator.setPreviousOptimumPath(optimumPath);
                        tentativeOperator.setOptimumPath(new HashMap<>());
                        tentativeOperator.setNodeNetworkValues(new HashMap<>());
                    }
                }
            } else {
                System.err.println("Invalid node. Node should be an active or tentative operator");
            }
        } else if(o instanceof StateTransferMessage) {
            StateTransferMessage transferMessage = (StateTransferMessage) o;
            Node optimumNode = transferMessage.getTransferTo();
            Node parentSourceNode = transferMessage.getSource();
            //System.out.println(node.getID()+" pe aya hu " + transferMessage.getSource().getID() + " se " + "Optimum Node hai " + optimumNode.getID());

            if(activeOperatorOptional.isPresent()) {
                ActiveOperator activeOperator = activeOperatorOptional.get();
                activeOperator.setParent(Optional.of(parentSourceNode));
                /*if optimum node is different from the current active operator node*/
                if(optimumNode != node) {
                    if (activeOperator.getTentativeOperatorList().contains(optimumNode)) {
                        /*Clone properties of the active operator that are to be transferred*/
                        ActiveOperator transferredActiveOperator = activeOperator.clone();
                        TransferActiveOperatorMessage transferOperatorMessage = new TransferActiveOperatorMessage();
                        transferOperatorMessage.setActiveOperator(transferredActiveOperator);

                        /*Unset all tentative operators of old active operator*/
                        activeOperator.getTentativeOperatorList().stream().filter(op -> op != optimumNode).forEach(tentativeOp -> {
                            AdaptiveCepProtocol tentativeProtocol = (AdaptiveCepProtocol) tentativeOp.getProtocol(pid);
                            tentativeProtocol.setTentativeOperator(Optional.empty());
                        });
                        transportProtocol.send(node, optimumNode, transferOperatorMessage, pid);
                        /*Empty the current active operator*/
                        cepProtocol.setActiveOperator(Optional.empty());
                    } else {
                        System.err.println("Tentative Operator not present in the list");
                    }
                } else {
                    /*Send state transfer message to the optimum child*/
                    HashMap<Node, HashMap<Node, NodeParameters>> finalOptimumPath = activeOperator.getPreviousOptimumPath();
                    List<Node> newDependencies = activeOperator.getDependencies().stream().map(dependency -> {
                        HashMap<Node, NodeParameters> optimumNodeMap = finalOptimumPath.get(dependency);
                        this.sendStateTransferMessage(optimumNodeMap, node, dependency, transportProtocol, pid);
                        return optimumNodeMap.keySet();
                    }).collect(Collectors.toList()).stream().flatMap(Set::stream)
                            .collect(Collectors.toList());
                    activeOperator.setDependencies(newDependencies);
                }
            } else {
                System.err.println("Should be an active operator only");
            }
        } else if(o instanceof TransferActiveOperatorMessage) {
            TransferActiveOperatorMessage transferredActiveOperatorMessage = (TransferActiveOperatorMessage) o;
            if(cepProtocol.getTentativeOperator().isPresent()) {
                TentativeOperator currentTentativeOperator = cepProtocol.getTentativeOperator().get();
                ActiveOperator transferredActiveOperator = transferredActiveOperatorMessage.getActiveOperator();

                Linkable linkable =
                        (Linkable) node.getProtocol(FastConfig.getLinkable(pid));
                if (linkable.degree() > 0) {
                    List tentativeOpList = transferredActiveOperator.getTentativeOperatorList();
                    for (int i = 0; i < linkable.degree(); i++) {
                        Node peern = linkable.getNeighbor(i);
                        AdaptiveCepProtocol peernProto = (AdaptiveCepProtocol) peern.getProtocol(pid);
                        if (!peernProto.getTentativeOperator().isPresent() && !peernProto.getActiveOperator().isPresent()) {
                            TentativeOperator top = new TentativeOperator();
                            top.setActiveOperator(Optional.of(node));
                            //store the reference of the tentaive operator
                            tentativeOpList.add(peern);
                            //set the neighbor node as tentative operator
                            peernProto.setTentativeOperator(Optional.of(top));
                        }
                    }
                    transferredActiveOperator.setTentativeOperatorList(tentativeOpList);

                    HashMap<Node, HashMap<Node, NodeParameters>> finalOptimumPath = currentTentativeOperator.getPreviousOptimumPath();
                    List<Node> newDependencies = transferredActiveOperator.getDependencies().stream().map(dependency -> {
                        HashMap<Node, NodeParameters> optimumNodeMap = finalOptimumPath.get(dependency);
                        this.sendStateTransferMessage(optimumNodeMap, node, dependency, transportProtocol, pid);
                        return optimumNodeMap.keySet();
                    }).collect(Collectors.toList()).stream().flatMap(Set::stream)
                            .collect(Collectors.toList());
                    transferredActiveOperator.setDependencies(newDependencies);

                    /*Change the tentative operator to active operator*/
                    cepProtocol.setTentativeOperator(Optional.empty());
                    cepProtocol.setActiveOperator(Optional.of(transferredActiveOperator));
                } else {
                    System.err.println("TransferActiveOperatorMessage: Node is not a tentative Operator");
                }
            }
        }

    }

    private void sendStateTransferMessage(HashMap<Node, NodeParameters> optimumNodeMap, Node node, Node dependency, Transport transportProtocol, int pid) {
        if(optimumNodeMap.size() > 1){
            System.err.println("Something went wrong. There should be only one optimum value");
        } else {
            optimumNodeMap.keySet().stream().forEach(optimumNode -> {
                StateTransferMessage transferMsg = new StateTransferMessage();
                transferMsg.setSource(node);
                transferMsg.setTransferTo(optimumNode);
                transportProtocol.send(node, dependency, transferMsg, pid);
            });
        }
    }

    private NodeParameters findOptimumValue(HashMap<Node, HashMap<Node, NodeParameters>> optimumPath, AdaptiveCepProtocol cepProtocol) {
        /*collect the optimum value from all the branches*/
        List<NodeParameters> op = optimumPath.values().stream().map(HashMap::values)
                .collect(Collectors.toList())
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        NodeParameters optimumValue = new NodeParameters();
        if(cepProtocol.isAdaptiveLatency() && cepProtocol.isAdaptiveBandwidth()) {
            /*select optimum value among all branches*/
            optimumValue = minmax("Maximizing", op);
        } else if(cepProtocol.isAdaptiveLatency()) {
            List<Double> latencies = op.stream()
                    .map(NodeParameters::getLatency).collect(Collectors.toList());
            Double optimumLatency = minmax("Minimizing", latencies);
            optimumValue.setLatency(optimumLatency);
        } else {
            List<Double> bandwidth = op.stream()
                    .map(NodeParameters::getBandwidth).collect(Collectors.toList());
            Double optimumBandwidth = minmax("Maximizing", bandwidth);
            optimumValue.setBandwidth(optimumBandwidth);
        }
        return optimumValue;
    }

    private HashMap<Node,HashMap<Node,NodeParameters>> findOptimizedPath(HashMap<Node, HashMap<Node, NodeParameters>> nodeParamValues,
                                                                          HashMap<Node, Integer> tentativeOperatorOfChildren,
                                                                          AdaptiveCepProtocol cepProtocol,
                                                                          HashMap<Node, HashMap<Node, NodeParameters>> optimumPath) {
        for (Node branch : nodeParamValues.keySet()) {
            HashMap<Node, NodeParameters> paramValues = nodeParamValues.get(branch);
            /*calculate the optimum value only when cost message is received from all the child active and tentative operators*/
            if (paramValues.size() == tentativeOperatorOfChildren.get(branch)) {
                Node optimumNode;
                /*find the optimized value among all nodes from a direction/branch*/
                if(cepProtocol.isAdaptiveLatency() && cepProtocol.isAdaptiveBandwidth()) {
                    optimumNode = minmaxBy("Maximizing", paramValues);
                } else if(cepProtocol.isAdaptiveLatency()) {
                    HashMap<Node, Double> latencyMap = (HashMap<Node, Double>) paramValues.entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().getLatency()
                    ));
                    optimumNode = minmaxBy("Minimizing", latencyMap);
                } else {
                    HashMap<Node, Double> bandiwdthMap = (HashMap<Node, Double>) paramValues.entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> e.getValue().getBandwidth()
                    ));
                    optimumNode = minmaxBy("Maximizing", bandiwdthMap);
                }

                HashMap<Node, NodeParameters> optimumValues = new HashMap<>();
                /*set in minimum as optimized path*/
                optimumValues.put(optimumNode, paramValues.get(optimumNode));
                optimumPath.put(branch, optimumValues);
            }
        }
        return optimumPath;
    }

    private void sendMessage(NodeParameters value, Node src, Node dest, AdaptiveCepProtocol cepProtocol, Integer pid) {
        Transport transportProtocol = ((Transport) src.getProtocol(FastConfig.getTransport(pid)));
        NodeParameters summedCost = new NodeParameters();
        if(cepProtocol.isAdaptiveLatency() && cepProtocol.isAdaptiveBandwidth()) {
            summedCost = mergeLatencyBandwidth(value ,hostProps.get((int)dest.getID()));
        } else if(cepProtocol.isAdaptiveLatency()) {
            Double latency = mergeLatency(value ,hostProps.get((int)dest.getID()));
            summedCost.setLatency(latency);
        } else {
            Double bandwidth = mergeBandwidth(value ,hostProps.get((int)dest.getID()));
            summedCost.setBandwidth(bandwidth);
        }
        transportProtocol.
                send(src, dest, new OptimumCostMessage(summedCost, src), pid);
    }
    private HashMap<Node, HashMap<Node, NodeParameters>> getUpdatedNodeParamValues(HashMap<Node, HashMap<Node, NodeParameters>> nodeParamValues,
                                                                                   OptimumCostMessage optimumCostMessage, Node messageSource, Integer pid) {
        AdaptiveCepProtocol msgSrcProtocol = (AdaptiveCepProtocol) messageSource.getProtocol(pid);
        /*check if source message node is active operator*/
        if(msgSrcProtocol.getActiveOperator().isPresent()) {
            HashMap<Node, NodeParameters> paramValues = nodeParamValues.getOrDefault(messageSource, new HashMap<>());
                /*store the cost message/network parameters in a map as (source -> cost message) pair*/
            paramValues.put(messageSource, optimumCostMessage.getParams());
                /*store the above map in another map which links from which
                direction/branch of the dependency tree the message was received.
                For example, a join operator will have 2 children, therefore it will receive cost messages from 2 directions*/
            nodeParamValues.put(messageSource, paramValues);
        }
            /*check if source message node is tentative operator*/
        else if(msgSrcProtocol.getTentativeOperator().isPresent()){
            TentativeOperator msgSrcTentativeOperator = msgSrcProtocol.getTentativeOperator().get();
            HashMap<Node, NodeParameters> paramValues = nodeParamValues.getOrDefault(msgSrcTentativeOperator.getActiveOperator().get()
                    , new HashMap<>());
                /*store the cost message/network parameters in a map as (source -> cost message) pair*/
            paramValues.put(messageSource, optimumCostMessage.getParams());
                /*If the message is received from tentative operator, it should be stored in the map with key as
                 active operator it is linked with. This helps in determining from which
                direction/branch of the dependency tree the message was received.
                For example, a join operator will have 2 children, therefore it will receive cost messages from 2 directions*/
            nodeParamValues.put(msgSrcTentativeOperator.getActiveOperator().get(), paramValues);
        } else {
            System.err.println("Error. Node should be an active or tentative operator");
        }
        return nodeParamValues;
    }

    double randomLatency()
    {
        return (2 + 98 * CommonState.r.nextDouble());
    }

    double randomBandwidth()
    {
        return (5 + 95 * CommonState.r.nextDouble());
    }

    void advance() {
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

    private Double mergeBandwidth(NodeParameters d1, NodeParameters d2){
        return Math.min(d1.getBandwidth(), d2.getBandwidth());
    }
    private Double mergeLatency(NodeParameters d1, NodeParameters d2){
        return d1.getLatency() + d2.getLatency();
    }
    private NodeParameters mergeLatencyBandwidth(NodeParameters d1, NodeParameters d2){
        NodeParameters response = new NodeParameters();
        response.setBandwidth(mergeBandwidth(d1, d2));
        response.setLatency(mergeLatency(d1, d2));
        return response;
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

}
