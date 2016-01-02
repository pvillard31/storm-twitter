
package fr.pvillard.storm.bolt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Counter bolt
 * 
 * @author pvillard
 */
@SuppressWarnings({"rawtypes", "serial"})
public class TweetCounterBolt extends BaseRichBolt {

    /** counter map */
    private Map<String, Long> counter;
    /** collector */
    private OutputCollector collector;
    /** filter keywords */
    private String[] filters;
    /** period */
    private int period;

    /**
     * Constructor
     * 
     * @param filters
     *            words to use for filtering
     * @param period
     *            period in seconds for tick frequency
     */
    public TweetCounterBolt(String[] filters, int period) {
        this.filters = filters;
        this.period = period;
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.task.IBolt#prepare(java.util.Map,
     *      backtype.storm.task.TopologyContext,
     *      backtype.storm.task.OutputCollector)
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.counter = new HashMap<String, Long>();
        for (String filter : this.filters) {
            this.counter.put(filter, 0L);
        }
        this.collector = collector;
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("filter", "tickdate", "totalcount")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.topology.base.BaseComponent#getComponentConfiguration()
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> map = super.getComponentConfiguration();
        if (map == null) {
            map = new HashMap<String, Object>();
        }
        map.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, this.period);
        return map;
    }

    /**
     * return true if the tuple is a tick
     * 
     * @param tuple
     *            tuple
     * @return true if the tuple is a tick
     */
    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
     */
    @Override
    public void execute(Tuple input) {

        if (isTickTuple(input)) {
            for (String filter : this.filters) {
                System.out.println("TICK with [" + filter + "]"); //$NON-NLS-1$ //$NON-NLS-2$
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm"); //$NON-NLS-1$
                Date date = new Date();
                this.collector.emit(new Values(filter, format.format(date) + ":00", this.counter.get(filter))); //$NON-NLS-1$
                // System.out.println("Number of tweet with [" + filter + "] at
                // [" + format.format(date) + "] : " + counter.get(filter));
                this.counter.put(filter, 0L);
            }
        } else {
            String filter = (String) input.getValueByField("filter"); //$NON-NLS-1$
            Long count = this.counter.get(filter);
            count = count == null ? 1L : count + 1;
            this.counter.put(filter, count);
        }

    }

}
