package com.chw.test.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 单词次数统计bolt
 * @author Administrator
 *
 */
public class WordCountBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8761807561458126413L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(WordCountBolt.class);

	private Map<String,Integer> countMap=new HashMap<String,Integer>();

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	}

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Integer count = countMap.get(word);
		if(count == null) {
			count = 0;
		}
		count++;
		countMap.put(word, count);
		LOGGER.info("【WordCountBolt完成单词次数统计】word=" + word + ", count=" + count);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
