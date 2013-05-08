package co.gridport.kafka.hadoop;

import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class KafkaInputRecordReader extends RecordReader<LongWritable, BytesWritable> {

    static Logger log = LoggerFactory.getLogger(KafkaInputRecordReader.class);

    private Configuration conf;

    private KafkaInputSplit split;
    //private TaskAttemptContext context;

    private SimpleConsumer consumer ;
    private int fetchSize;
    private String topic;
    private String reset;

    private int partition;
    private long earliestOffset;
    private long watermark;
    private long latestOffset;

    private ByteBufferMessageSet messages;
    private Iterator<MessageAndOffset> iterator;
    private LongWritable key;
    private BytesWritable value;

    private long numProcessedMessages = 0L;

    private String clientId = null;

    // return UUID based clientId
    private synchronized String getClientId() {
        if (clientId == null) {
            clientId = "KafkaInputRecordReader" + UUID.randomUUID();
        }
        return clientId;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        initialize(split, context.getConfiguration());
    }

    public void initialize(InputSplit split, Configuration conf) throws IOException, InterruptedException
    {
        this.conf = conf;
        this.split = (KafkaInputSplit) split;
        topic = this.split.getTopic();
        partition = this.split.getPartition();
        watermark = this.split.getWatermark();

        int timeout = conf.getInt("kafka.socket.timeout.ms", 30000);
        int bufferSize = conf.getInt("kafka.socket.buffersize", 64*1024);
        consumer =  new SimpleConsumer(this.split.getBrokerHost(), this.split.getBrokerPort(), timeout, bufferSize, getClientId());

        fetchSize = conf.getInt("kafka.fetch.size", 1024 * 1024);
        reset = conf.get("kafka.watermark.reset", "watermark");
        earliestOffset = getEarliestOffset();
        latestOffset = getLatestOffset();

        //log.info("Last watermark for {} to {}", topic +":"+partition, watermark);

        if ("earliest".equals(reset)) {
            resetWatermark(-1);
        } else if("latest".equals(reset)) {
            resetWatermark(latestOffset);
        } else if (watermark < earliestOffset) {
            resetWatermark(-1);
        }

        log.info(
            "Split {} Topic: {} Broker: {} Partition: {} Earliest: {} Latest: {} Starting: {}", 
            new Object[]{this.split, topic, this.split.getBrokerId(), partition, earliestOffset, latestOffset, watermark }
        );
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable();
        }

        if (messages == null) {
// correlationId : scala.Int, clientId : scala.Predef.String, maxWait : scala.Int, minBytes : scala.Int, requestInfo : java.util.Map[kafka.common.TopicAndPartition, kafka.api.PartitionFetchInfo]
            int correlationId = 1;
            int maxWait = 10000;
            int minBytes = 0;
            Map<TopicAndPartition, PartitionFetchInfo> requestInfo = new HashMap<TopicAndPartition, PartitionFetchInfo>();
            TopicAndPartition tp = new TopicAndPartition(topic, partition);
            PartitionFetchInfo partOffset = new PartitionFetchInfo(-2L, 1);
            requestInfo.put(tp, partOffset);

            FetchRequest request = new FetchRequest(correlationId, getClientId(), maxWait, minBytes, requestInfo);

            log.info("{} fetching offset {} ", topic+":" + split.getBrokerId() +":" + partition, watermark);
            FetchResponse response = consumer.fetch(request);

            if (response.hasError()) {
                log.info(response.toString());
                return false;
            }
            else {
                messages = response.messageSet(topic, partition);
                iterator = response.messageSet(topic, partition).iterator();
                watermark += messages.validBytes();
                if (!iterator.hasNext())
                {
                    //log.info("No more messages");
                    return false;
                }
            }
        }

        if (iterator.hasNext())
        {
            MessageAndOffset messageOffset = iterator.next();
            Message message = messageOffset.message();
            key.set(watermark - message.size() - 4);
            value.set(message.payload().array(), message.payload().arrayOffset(), message.payloadSize());
            numProcessedMessages++;
            if (!iterator.hasNext())
            {
                messages = null;
                iterator = null;
            }
            return true;
        }
        log.warn("Unexpected iterator end.");
        return false;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException
    {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException
    {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException 
    {
        if (watermark >= latestOffset || earliestOffset == latestOffset) {
            return 1.0f;
        }
        return Math.min(1.0f, (watermark - earliestOffset) / (float)(latestOffset - earliestOffset));
    }

    @Override
    public void close() throws IOException
    {
        log.info("{} num. processed messages {} ", topic+":" + split.getBrokerId() +":" + partition, numProcessedMessages);
        if (numProcessedMessages >0)
        {
            ZkUtils zk = new ZkUtils(
                conf.get("kafka.zk.connect"),
                conf.getInt("kafka.zk.sessiontimeout.ms", 10000),
                conf.getInt("kafka.zk.connectiontimeout.ms", 10000)
            );

            String group = conf.get("kafka.groupid");
            String partition = split.getBrokerId() + "-" + split.getPartition();
            zk.commitLastConsumedOffset(group, split.getTopic(), partition, watermark);
            zk.close();
        }
        consumer.close();
    }

    private long getOffset(String topic, int partition, Long time, int maxOffset) {
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        TopicAndPartition tp = new TopicAndPartition(topic, partition);
        PartitionOffsetRequestInfo partOffset = new PartitionOffsetRequestInfo(time, maxOffset);
        requestInfo.put(tp, partOffset);

        OffsetRequest request = new OffsetRequest(requestInfo, (short) 1, getClientId());
        OffsetResponse response = consumer.getOffsetsBefore(request);
        return response.offsets(topic, partition)[0];
    }

    private long getEarliestOffset() {
        if (earliestOffset <= 0) {
            earliestOffset = getOffset(topic, partition, -2L, 1);
        }
        return earliestOffset;
    }

    private long getLatestOffset() {
        if (latestOffset <= 0) {
            latestOffset = getOffset(topic, partition, -1L, 1);
        }
        return latestOffset;
    }

    private void resetWatermark(long offset) {
        if (offset <= 0) {
            offset = getOffset(topic, partition, -1L, 1);
        }
        log.info("{} resetting offset to {}", topic+":" + split.getBrokerId() +":" + partition, offset);
        watermark = earliestOffset = offset;
    }

}
