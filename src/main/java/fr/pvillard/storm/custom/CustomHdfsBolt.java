
package fr.pvillard.storm.custom;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.hdfs.bolt.HdfsBolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

/**
 * Custom overriding of the bolt to specify the correct HDFS configuration files
 * 
 * @author pvillard
 */
@SuppressWarnings("serial")
public class CustomHdfsBolt extends HdfsBolt {

    /**
     * {@inheritDoc}
     * 
     * @see org.apache.storm.hdfs.bolt.HdfsBolt#doPrepare(java.util.Map,
     *      backtype.storm.task.TopologyContext,
     *      backtype.storm.task.OutputCollector)
     */
    @Override
    @SuppressWarnings("rawtypes")
    public void doPrepare(Map conf, TopologyContext topologyContext, OutputCollector collector) throws IOException {
        this.hdfsConfig.addResource(new Path("file:///usr/hdp/current/hadoop-client/conf/hdfs-site.xml")); //$NON-NLS-1$
        this.hdfsConfig.addResource(new Path("file:///usr/hdp/current/hadoop-client/conf/core-site.xml")); //$NON-NLS-1$
        this.fs = FileSystem.get(URI.create(this.fsUrl), this.hdfsConfig);
    }

}
