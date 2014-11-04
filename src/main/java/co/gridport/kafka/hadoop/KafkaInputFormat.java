package co.gridport.kafka.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {

	private Set<String> partitions = new HashSet<String>();
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {

		List<InputSplit> splits = new ArrayList<InputSplit>();

		Configuration conf = context.getConfiguration();
		List<String> topics = Arrays.asList(conf.get("kafka.topics").split(","));
		ZkUtils zk = new ZkUtils(conf.get("kafka.zk.connect"), conf.getInt("kafka.zk.sessiontimeout.ms", 10000), conf.getInt("kafka.zk.connectiontimeout.ms",
				10000));
		List<String> seeds = zk.getSeedList();

		String consumerGroup = conf.get("kafka.groupid");

		for (final String seed : seeds) {
			String[] hs = seed.split(":");
			SimpleConsumer consumer = new SimpleConsumer(hs[0], Integer.parseInt(hs[1]), 10000, 65535, "PartitionsLookup");
			TopicMetadataRequest request = new TopicMetadataRequest(topics);
			TopicMetadataResponse response = consumer.send(request);
			if (response != null && response.topicsMetadata() != null) {
				for (TopicMetadata tm : response.topicsMetadata()) {
					for (PartitionMetadata pm : tm.partitionsMetadata()) {
						String key = tm.topic() + "-" + pm.partitionId();
						if (!partitions.contains(key)) {
							long lastConsumedOffset = zk.getLastConsumedOffset(consumerGroup, tm.topic(), pm.partitionId());
							InputSplit split = new KafkaInputSplit(seed, tm.topic(), pm.partitionId(), lastConsumedOffset);
							splits.add(split);
							partitions.add(key);
						}
					}
				}
			}
		}
		zk.close();
		return splits;
	}

	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new KafkaInputRecordReader();
	}

}
