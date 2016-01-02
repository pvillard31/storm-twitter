
package fr.pvillard.storm.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Reads Twitter's feed using the twitter4j library.
 * 
 * @author pvillard
 */
@SuppressWarnings({"rawtypes", "serial"})
public class TwitterSpout extends BaseRichSpout {

    /** collector */
    private SpoutOutputCollector collector;
    /** queue for tweets */
    private LinkedBlockingQueue<Status> queue;
    /** stream */
    private TwitterStream twitterStream;
    /** words for filtering */
    private String[] filters;
    /** Twitter API Consumer Key */
    private String consumerKey;
    /** Twitter API Consumer Secret */
    private String consumerSecret;
    /** Twitter API Access Token */
    private String accessToken;
    /** Twitter API Access Token Secret */
    private String accessTokenSecret;

    /**
     * constructor
     * 
     * @param filters
     *            filters
     * @param consumerKey
     *            consumer key
     * @param consumerSecret
     *            consumer secret
     * @param accessToken
     *            access token
     * @param accessTokenSecret
     *            access token secret
     */
    public TwitterSpout(String[] filters, String consumerKey, String consumerSecret, String accessToken,
            String accessTokenSecret) {
        this.filters = filters;
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.spout.ISpout#open(java.util.Map,
     *      backtype.storm.task.TopologyContext,
     *      backtype.storm.spout.SpoutOutputCollector)
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.queue = new LinkedBlockingQueue<Status>(1000);
        this.collector = collector;

        StatusListener listener = new StatusListener() {
            /**
             * 
             * {@inheritDoc}
             * 
             * @see twitter4j.StatusListener#onStatus(twitter4j.Status)
             */
            @Override
            public void onStatus(Status status) {
                TwitterSpout.this.queue.offer(status);
            }

            /**
             * 
             * {@inheritDoc}
             * 
             * @see twitter4j.StatusListener#onDeletionNotice(twitter4j.StatusDeletionNotice)
             */
            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            /**
             * 
             * {@inheritDoc}
             * 
             * @see twitter4j.StatusListener#onTrackLimitationNotice(int)
             */
            @Override
            public void onTrackLimitationNotice(int i) {
            }

            /**
             * 
             * {@inheritDoc}
             * 
             * @see twitter4j.StatusListener#onScrubGeo(long, long)
             */
            @Override
            public void onScrubGeo(long l, long l1) {
            }

            /**
             * 
             * {@inheritDoc}
             * 
             * @see twitter4j.StatusListener#onStallWarning(twitter4j.StallWarning)
             */
            @Override
            public void onStallWarning(StallWarning stallWarning) {
            }

            /**
             * 
             * {@inheritDoc}
             * 
             * @see twitter4j.StreamListener#onException(java.lang.Exception)
             */
            @SuppressWarnings("javadoc")
            @Override
            public void onException(Exception e) {
            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(false).setOAuthConsumerKey(this.consumerKey).setOAuthConsumerSecret(this.consumerSecret)
                .setOAuthAccessToken(this.accessToken).setOAuthAccessTokenSecret(this.accessTokenSecret);
        TwitterStreamFactory factory = new TwitterStreamFactory(cb.build());
        this.twitterStream = factory.getInstance();
        this.twitterStream.addListener(listener);
        this.twitterStream.filter(this.filters);
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.spout.ISpout#nextTuple()
     */
    @Override
    public void nextTuple() {
        Status ret = this.queue.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            this.collector.emit(new Values(ret));
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.topology.base.BaseRichSpout#close()
     */
    @Override
    public void close() {
        this.twitterStream.shutdown();
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.topology.base.BaseComponent#getComponentConfiguration()
     */
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    /**
     * {@inheritDoc}
     * 
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet")); //$NON-NLS-1$
    }

}
