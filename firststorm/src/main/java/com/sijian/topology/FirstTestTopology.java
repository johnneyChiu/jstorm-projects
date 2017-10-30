package com.sijian.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirstTestTopology {

    private static Logger LOG = LoggerFactory.getLogger(FirstTestTopology.class);

    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_BOLT_PARALLELISM_HINT  = "bolt.parallel";

    public static void main(String ... args){
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //设置数据源
        //设置多少个executor来执行spout
        topologyBuilder.setSpout("test-spout", new FirstTestSpout(), 1);
        //setBolt:SplitBolt的grouping策略是上层随机分发，CountBolt的grouping策略是按照上层字段分发
        //如果想要从多个Bolt获取数据，可以继续设置grouping
        topologyBuilder.setBolt("test-split-bolt", new FirstTestBoltWordSplit(), 1)
                .localOrShuffleGrouping("test-spout");
        topologyBuilder.setBolt("test-count-bolt", new FirstTestBoltWordCount(), 1)
                .fieldsGrouping("test-split-bolt", new Fields("word"))
                ;
        Config config = new Config();
        config.setDebug(false);
        if (args.length > 2 && args != null) {
            config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
            config.setNumAckers(1);
            config.setNumWorkers(1);
            config.put("infile", args[1]);
            config.put("outfile", args[2]);
            config.put("starting", "count is starting!");
            try {
                StormSubmitter.submitTopology(args[0],config,topologyBuilder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }
    }
}
