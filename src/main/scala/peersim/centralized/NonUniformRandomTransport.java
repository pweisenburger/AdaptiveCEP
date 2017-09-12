package peersim.centralized;

import peersim.config.*;
import peersim.core.*;
import peersim.edsim.*;
import peersim.transport.Transport;

/**
 * Created by pratik_k on 4/10/2017.
 */
public class NonUniformRandomTransport implements Transport {

//---------------------------------------------------------------------
//Parameters
//---------------------------------------------------------------------

    /**
     * String name of the parameter used to configure the minimum latency.
     * @config
     */
    private static final String PAR_MINDELAY = "mindelay";

    /**
     * String name of the parameter used to configure the maximum latency.
     * Defaults to {@value #PAR_MINDELAY}, which results in a constant delay.
     * @config
     */
    private static final String PAR_MAXDELAY = "maxdelay";

    /**
     * String name of the parameter used to configure the nodes having the lowest latency.
     * Defaults to {@value #PAR_NODESWITHLOWESTDELAY}, which results in a {@value #PAR_NODESWITHLOWESTDELAY} nodes
     * having delay between {@value #PAR_MINLOWESTDELAY} and {@value #PAR_MAXLOWESTDELAY}.
     * @config
     */
    private static final String PAR_NODESWITHLOWESTDELAY = "nodesWithLowestDelay";

    /**
     * String name of the parameter used to configure the lowest latency for {@value #PAR_NODESWITHLOWESTDELAY} nodes.
     * @config
     */
    private static final String PAR_MINLOWESTDELAY = "minlowestdelay";

    /**
     * String name of the parameter used to configure the lowest latency for {@value #PAR_NODESWITHLOWESTDELAY} nodes.
     * Defaults to {@value #PAR_MINLOWESTDELAY}, which results in a few nodes having delay between
     * {@value #PAR_MINLOWESTDELAY} and {@value #PAR_MAXLOWESTDELAY}.
     * @config
     */
    private static final String PAR_MAXLOWESTDELAY = "maxlowestdelay";


    /**
     * String name of the parameter used to configure low bandwidth.
     * @config
     */
    private static final String PAR_LOWBANDWIDTH = "lowbandwidth";

    /**
     * String name of the parameter used to configure high bandwidth.
     * Defaults to {@value #PAR_LOWBANDWIDTH}, which results in a constant bandwidth.
     * @config
     */
    private static final String PAR_HIGHBANDWIDTH = "highbandwidth";
//---------------------------------------------------------------------
//Fields
//---------------------------------------------------------------------

    /** Minimum delay for message sending */
    private final long min;

    /** Difference between the max and min delay plus one. That is, max delay is
     * min+range-1.
     */
    private final long range;

    /** Min Lowest delay for message sending */
    private final long minLowestDelay;

    /** Difference between the maxLowestDelay and minLowestDelay delay plus one. */
    private final long lowestRange;

    /** Minimum delay for message sending */
    private final long noOfNodeWithLowestDelay;

    private final long modulo;

    /** Low Bandwidth for nodes*/
    private final long lowbandwidth;

    /** High Bandwidth for nodes. */
    private final long highbandiwdth;


//---------------------------------------------------------------------
//Initialization
//---------------------------------------------------------------------

    /**
     * Reads configuration parameter.
     */
    public NonUniformRandomTransport(String prefix)
    {
        min = Configuration.getLong(prefix + "." + PAR_MINDELAY);
        minLowestDelay = Configuration.getLong(prefix + "." + PAR_MINLOWESTDELAY);
        noOfNodeWithLowestDelay = Configuration.getLong(prefix + "." + PAR_NODESWITHLOWESTDELAY);
        long max = Configuration.getLong(prefix + "." + PAR_MAXDELAY,min);
        long maxLowestRange = Configuration.getLong(prefix + "." + PAR_MAXLOWESTDELAY,minLowestDelay);
        if (max < min)
            throw new IllegalParameterException(prefix+"."+PAR_MAXDELAY,
                    "The maximum latency cannot be smaller than the minimum latency");
        range = max-min+1;
        lowestRange = maxLowestRange - minLowestDelay + 1;
        modulo = Math.floorDiv(Network.size(),noOfNodeWithLowestDelay);
        lowbandwidth = Configuration.getLong(prefix + "." + PAR_LOWBANDWIDTH);
        highbandiwdth = Configuration.getLong(prefix + "." + PAR_HIGHBANDWIDTH, lowbandwidth);
    }

//---------------------------------------------------------------------

    /**
     * Returns <code>this</code>. This way only one instance exists in the system
     * that is linked from all the nodes. This is because this protocol has no
     * node specific state.
     */
    public Object clone()
    {
        return this;
    }

//---------------------------------------------------------------------
//Methods
//---------------------------------------------------------------------

    /**
     * Delivers the message with a random
     * delay, that is drawn from the configured interval according to the uniform
     * distribution.
     */
    public void send(Node src, Node dest, Object msg, int pid)
    {
        // avoid calling nextLong if possible
        long delay = (range==1?min:min + CommonState.r.nextLong(range));
        EDSimulator.add(delay, msg, dest, pid);
    }

    /**
     * Returns a random
     * delay, that is drawn from the configured interval according to the random
     * distribution.
     */
    public long getLatency(Node src, Node dest)
    {
        if(src.getID() % modulo == 1) {
            return (range==1?min:minLowestDelay + CommonState.r.nextLong(lowestRange));
        }
        return (range==1?min:min + CommonState.r.nextLong(range));
    }

    /**
     * Returns a random
     * bandwidth, that is drawn from the configured interval according to the random
     * distribution.
     */
    public long getBandwidth(Node src, Node dest)
    {
        if(src.getID() % modulo == 1) {
            return (range==1?min:lowbandwidth + CommonState.r.nextLong(lowbandwidth));
        }
        return (range==1?min:highbandiwdth + CommonState.r.nextLong(highbandiwdth));
    }

    public NodeParameters getNodeParameters(Node src, Node dest)
    {
        NodeParameters response = new NodeParameters();
        if(src.getID() % modulo == 1) {
            response.setBandwidth((double) (highbandiwdth + CommonState.r.nextLong(highbandiwdth)));
        } else {
            response.setBandwidth((double) (lowbandwidth + CommonState.r.nextLong(highbandiwdth)));
        }
        if(src.getID() % modulo == 1) {
            response.setLatency((double) (minLowestDelay + CommonState.r.nextLong(lowestRange)));
        } else {
            response.setLatency((double) (min + CommonState.r.nextLong(range)));
        }
        //response.setBandwidth((double) getBandwidth(src, dest));
        //response.setLatency((double)getLatency(src, dest));
        return response;
    }

}
