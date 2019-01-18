package buaa.act.ucar.kafkaopt;

import com.zuche.us.thrift.ThriftObdGps;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaGPSConsumer implements Runnable {
    private String topic;

    public KafkaGPSConsumer(String topic) {
        this.topic = topic;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.6.131:2181,192.168.6.132:2181,192.168.6.133:2181");
        props.put("group.id", "playback-test-2");
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("auto.commit.enable", "true");
        props.put("auto.offset.reset", "largest");
        props.put("auto.commit.interval.ms", "60000");
        return new ConsumerConfig(props);
    }

    @Override
    public void run() {
        TDeserializer tDeserializer = new TDeserializer(new TCompactProtocol.Factory());
        // TODO Auto-generated method stub
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
        Map<String, Integer> topicMap = new HashMap<>();

        topicMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topicMap);
        KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        byte[] bs;
        try {
            long count = 0L;
            while (true) {
                try {
                    if (it.hasNext()) {
                        ThriftObdGps thriftObdGps = new ThriftObdGps();
                        MessageAndMetadata<byte[], byte[]> item = it.next();
                        bs = item.message();
                        try {
                            tDeserializer.deserialize(thriftObdGps, bs);
                        } catch (TException e) {
                            e.printStackTrace();
                        }
                        if (null == thriftObdGps.getSn()) {
                            continue;
                        }
                        count++;
                        if(count % 1000 == 0) {
                            System.out.println(new Timestamp(thriftObdGps.getGpstime() * 1000) + ", " + thriftObdGps.toString());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            stream.clear();
            streamMap.clear();
            consumer.shutdown();
        }
    }

    public static void main(String[] args) {
        new Thread(new KafkaGPSConsumer("ThriftObdGps")).start();
    }
}
