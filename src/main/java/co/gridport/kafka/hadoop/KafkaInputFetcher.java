package co.gridport.kafka.hadoop;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class KafkaInputFetcher {
    static Logger log = LoggerFactory.getLogger(KafkaInputRecordReader.class);

    private List<String> seeds;

    private String clientId;
    private String topic;
    private Integer partition;

    private Broker leader;
    private List<String> replicaBrokers = new ArrayList<String>();
    private SimpleConsumer consumer = null;
    private Long offset;
    private ConcurrentLinkedQueue<MessageAndOffset> messageQueue;

    public KafkaInputFetcher(String clientId, String topic, int partition, List<String> seeds, int timeout, int bufferSize) {
        this.clientId = clientId;
        this.topic = topic;
        this.partition = partition;
        this.seeds = seeds;
        messageQueue =  new ConcurrentLinkedQueue<MessageAndOffset>();
        log.info("KafkaInputFetcher seed: " + seeds.toString());
    }

    public MessageAndOffset nextMessageAndOffset(Integer fetchSize) throws IOException {
        log.info("MessageAndOffset seed: " + seeds.toString());
        if (leader == null) {
        	log.info("MessageAndOffset before findLeader: " + seeds.toString());
            leader = findLeader(seeds);
        }
        if (consumer == null) {
            consumer = new SimpleConsumer(leader.host(), leader.port(), 10000, 65535, clientId);
        }
        if (offset == null) {
        	offset = getOffset(kafka.api.OffsetRequest.LatestTime());
        }
        if (messageQueue.size() < 100) {
            log.info("{} fetching offset {} ", topic+":" + partition, offset);
            FetchRequestBuilder requestBuilder = new FetchRequestBuilder();
            FetchRequest req = requestBuilder
                .clientId(clientId)
                .addFetch(topic, partition, offset, fetchSize)
                .build();
            FetchResponse response = consumer.fetch(req);
            if (response.hasError()) {
                short code = response.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leader.host() + ":" + leader.port() + " Reason: " + code);
                close();
                leader = findNewLeader(leader);
            }
            for (MessageAndOffset messageAndOffset : response.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < offset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + offset);
                    continue;
                }
                log.info("messageAndOffset: " + messageAndOffset.toString());
                messageQueue.offer(messageAndOffset);
            }
        }
        if (messageQueue.size() > 0) {
            MessageAndOffset messageAndOffset = messageQueue.poll();
            offset = messageAndOffset.nextOffset();
            return messageAndOffset;
        } else {
            return null;
        }

    }

    public Long getOffset(Long time) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientId);
        log.info("getOffset seed: " + seeds.toString());
        if (leader == null) {
        	log.info("getOffset findLeader: " + seeds.toString());
            leader = findLeader(seeds);
        }
        if (consumer == null) {
            consumer = new SimpleConsumer(leader.host(), leader.port(), 10000, 65535, clientId);
            log.info("Consumer: " + consumer.toString());
        }
        OffsetResponse response1 = consumer.getOffsetsBefore(request);

        if (response1.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response1.errorCode(topic, partition) );
            System.exit(1);
        }
        for (Long i : response1.offsets(topic, partition)) {
        	log.info("Printing Offset: " + i.toString());	
        }
        return response1.offsets(topic, partition)[0];
    }

    private Broker findNewLeader(Broker oldLeader) throws IOException {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            Broker newLeader= findLeader(replicaBrokers);
            if (newLeader == null) {
                goToSleep = true;
            } else if (oldLeader.host().equalsIgnoreCase(newLeader.host()) && oldLeader.port() == newLeader.port() && i == 0) {
                goToSleep = true;
            } else {
                return newLeader;
            }
            if (goToSleep) {
                try { Thread.sleep(1000); } catch (InterruptedException ie) {}
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new IOException("Unable to find new leader after Broker failure. Exiting");
    }

    private Broker findLeader(List<String> seeds) {
        PartitionMetadata returnMetaData = null;
        for (String seed : seeds) {
        	log.info("seed: " + seed);
            SimpleConsumer consumer = null;
            try {
            	String[] hs = seed.split(":");
                consumer = new SimpleConsumer(hs[0], Integer.parseInt(hs[1]), 100000, 64 * 1024, "leaderLookup");
                List<String> topics = new ArrayList<String>();
                topics.add(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null && returnMetaData.leader() != null) {
        	
            replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                replicaBrokers.add(replica.host()+":"+replica.port());
            }
            return returnMetaData.leader();
        } else {
            return null;
        }
    }

    public void close() {
       if (consumer != null) {
           consumer.close();
           consumer = null;
       }
    }
}
