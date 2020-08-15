package com.chw.test.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * 句子切割的bolt
 * @author Administrator
 *
 */
public class SentenceParseBolt extends BaseRichBolt {

	private static final long serialVersionUID = -8017609899644290359L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(SentenceParseBolt.class);

	private OutputCollector collector;

	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	public void execute(Tuple tuple) {
		String sentence = tuple.getStringByField("sentence");
		LOGGER.info("【SentenceParseBolt接收到一条句子】sentence=" + sentence);
		if(sentence!=null){
			sentence=sentence.trim();
			if(sentence.length()>0){
				String[] split = sentence.split(" ");
				for (String s : split) {
					s=s.trim();
					if(s.length()>0){
						collector.emit(new Values(s));
					}
				}
			}
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
