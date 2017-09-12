package peersim.distributed;

import peersim.config.Configuration;
import peersim.core.Protocol;

/**
 * Created by pratik_k on 7/4/2017.
 */
public class StaticValuesHolder implements Protocol {
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
     *	Adaptive Latency identifier, obtained from config property
     */
    private final boolean adaptiveLatency;

    /**
     *	Adaptive Bandwidth identifier, obtained from config property
     */
    private final boolean adaptiveBandwidth;

    public StaticValuesHolder(String prefix) {
        adaptiveLatency = Configuration.getBoolean(prefix + "." + PAR_ADAPTIVE_LATENCY);
        adaptiveBandwidth = Configuration.getBoolean(prefix + "." + PAR_ADAPTIVE_BANDWIDTH);
    }

    public boolean isAdaptiveLatency() {
        return adaptiveLatency;
    }

    public boolean isAdaptiveBandwidth() {
        return adaptiveBandwidth;
    }

    public Object clone() {
        StaticValuesHolder var1 = null;

        try {
            var1 = (StaticValuesHolder)super.clone();
        } catch (CloneNotSupportedException var3) {
            ;
        }

        return var1;
    }
}
