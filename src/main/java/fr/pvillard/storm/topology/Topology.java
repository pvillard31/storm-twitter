
package fr.pvillard.storm.topology;

import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import fr.pvillard.storm.bolt.TweetCounterBolt;
import fr.pvillard.storm.bolt.TweetSplitterBolt;
import fr.pvillard.storm.custom.CustomHdfsBolt;
import fr.pvillard.storm.spout.TwitterSpout;

/**
 * Topology class that sets up the Storm topology.
 */
public class Topology {

    /** name of the topology */
    static final String TOPOLOGY_NAME = "storm-twitter"; //$NON-NLS-1$
    /** words for filtering */
    static final String[] FILTERS = new String[]{"hortonworks", "hadoop"}; //$NON-NLS-1$ //$NON-NLS-2$
    /** metastore URI for Hive */
    static final String META_STORE_URI = "thrift://ip-172-31-30-70.eu-central-1.compute.internal:9083"; //$NON-NLS-1$
    /** cluster is HA */
    static final String HDFS = "hdfs://mycluster"; //$NON-NLS-1$
    /** Hive database */
    static final String DATABASE = "default"; //$NON-NLS-1$
    /** Hive table */
    static final String TABLE = "tweet_counts"; //$NON-NLS-1$
    /** Tick frequency in seconds */
    static final int PERIOD = 300;

    /**
     * Launch method
     * 
     * @param args
     *            arguments : <type : cluster/local> <Twitter consumer key>
     *            <Twitter consumer secret> <Twitter access token> <Twitter
     *            access token secret>
     */
    public static void main(String[] args) {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);

        String type = args[0];
        String consumerKey = args[1];
        String consumerSecret = args[2];
        String accessToken = args[3];
        String accessTokenSecret = args[4];

        TopologyBuilder b = new TopologyBuilder();
        b.setSpout("TwitterSpout", //$NON-NLS-1$
                new TwitterSpout(FILTERS, consumerKey, consumerSecret, accessToken, accessTokenSecret), 1);
        b.setBolt("TweetSplitterBolt", new TweetSplitterBolt(FILTERS), 1).shuffleGrouping("TwitterSpout"); //$NON-NLS-1$ //$NON-NLS-2$
        b.setBolt("TweetCounterBolt", new TweetCounterBolt(FILTERS, PERIOD), FILTERS.length) //$NON-NLS-1$
                .fieldsGrouping("TweetSplitterBolt", new Fields("filter")); //$NON-NLS-1$ //$NON-NLS-2$

        Fields fields = new Fields("filter", "tickdate", "totalcount"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        // define bolt to store data in HDFS
        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPrefix("twitter").withExtension(".txt") //$NON-NLS-1$ //$NON-NLS-2$
                .withPath("/storm"); //$NON-NLS-1$

        RecordFormat recordFormat = new DelimitedRecordFormat().withFields(fields);
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);
        SyncPolicy syncPolicy = new CountSyncPolicy(10);

        CustomHdfsBolt bolt = (CustomHdfsBolt) new CustomHdfsBolt().withFsUrl(HDFS).withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

        b.setBolt("TweetHdfsBolt", bolt, 1).allGrouping("TweetCounterBolt"); //$NON-NLS-1$ //$NON-NLS-2$

        // define bolt to store data in Hive
        DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper().withColumnFields(fields);
        HiveOptions hiveOptions = new HiveOptions(META_STORE_URI, DATABASE, TABLE, mapper).withTxnsPerBatch(2)
                .withBatchSize(10).withIdleTimeout(10);
        HiveBolt hiveBolt = new HiveBolt(hiveOptions);

        b.setBolt("TweetHiveBolt", hiveBolt, 1).allGrouping("TweetCounterBolt"); //$NON-NLS-1$ //$NON-NLS-2$

        switch (type) {
            case "cluster" : //$NON-NLS-1$
                try {
                    StormSubmitter.submitTopology(TOPOLOGY_NAME, config, b.createTopology());
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                break;
            case "local" : //$NON-NLS-1$
                final LocalCluster cluster = new LocalCluster();
                cluster.submitTopology(TOPOLOGY_NAME, config, b.createTopology());

                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        cluster.killTopology(TOPOLOGY_NAME);
                        cluster.shutdown();
                    }
                });
                break;
            default :
                System.out.println("Launch type incorrect"); //$NON-NLS-1$
                System.exit(1);
                break;

        }
    }

}
