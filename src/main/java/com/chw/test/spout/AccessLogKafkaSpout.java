package com.chw.test.spout;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费数据的spout
 */
public class AccessLogKafkaSpout extends BaseRichSpout {

	private static final long serialVersionUID = 8698470299234327074L;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AccessLogKafkaSpout.class);

	private SpoutOutputCollector collector;

	private KafkaConsumer<String, String> consumer;
	

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		String topicName="lyn";
		String groupId="test-consumer-group";
		Properties props = new Properties();
		props.put("bootstrap.servers", "zookeeper_test:9092,kafka_test:9092,nginx_test:9092");
		props.put("group.id", groupId);
		props.put("enable.auto.commit","true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset","earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Collections.singletonList(topicName));
		LOGGER.info("---------Kafka消费者启动---------");
		this.consumer=consumer;
	}

	public void nextTuple() {
		ConsumerRecords<String,String> records = consumer.poll(1000);
		for (ConsumerRecord<String,String> record : records) {
			LOGGER.info("分区："+record.partition()+" offset:"+record.offset()+"key:"+record.key()+"value:"+record.value());
			collector.emit(new Values(record.value()));
		}
	}
	 
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));
	}
	
}
