package com.chw.test;


import com.chw.test.bolt.SentenceParseBolt;
import com.chw.test.bolt.WordCountBolt;
import com.chw.test.spout.AccessLogKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 单词统计拓扑
 * @author Administrator
 *
 */
public class WordCountTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
	
		builder.setSpout("AccessLogKafkaSpout", new AccessLogKafkaSpout(), 1);
		builder.setBolt("SentenceParseBolt", new SentenceParseBolt(), 2)
				.setNumTasks(2)
				.shuffleGrouping("AccessLogKafkaSpout");
		builder.setBolt("WordCountBolt", new WordCountBolt(), 2)
				.setNumTasks(2)
				.fieldsGrouping("SentenceParseBolt", new Fields("word"));
		
		Config config = new Config();
		
		if(args != null && args.length > 0) {
			config.setNumWorkers(3);
			try {
				StormSubmitter.submitTopology(args[0], config, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("WordCountTopology", config, builder.createTopology());
			Utils.sleep(30000);
			cluster.shutdown();
		}
	}
	
}
