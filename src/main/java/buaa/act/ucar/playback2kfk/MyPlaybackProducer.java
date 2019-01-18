package buaa.act.ucar.playback2kfk;

import com.zuche.us.thrift.ThriftObdDs;
import com.zuche.us.thrift.ThriftObdError;
import com.zuche.us.thrift.ThriftObdEvent;
import com.zuche.us.thrift.ThriftObdGps;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class MyPlaybackProducer<T> implements Runnable {
    private String topic;
    private LinkedBlockingQueue<T> queue;

    public MyPlaybackProducer(String topic, LinkedBlockingQueue<T> queue) {
        this.topic = topic;
        this.queue = queue;
    }

    @Override
    public void run() {
        TSerializer ts = new TSerializer(new TCompactProtocol.Factory());
        // config kafka
        Producer<String, byte[]> producer;
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.6.131:2181,192.168.6.132:2181,192.168.6.133:2181");
        props.put("metadata.broker.list", "192.168.6.127:9092,192.168.6.128:9092,192.168.6.129:9092,192.168.6" +
                ".130:9092");
        props.put("request.required.acks", "-1");
        props.put("socket.timeout.ms", "30*1000");// Generated automatically if not set.
        // props.put("serializer.class","kafka.serializer.StringEncoder");//配置value的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");// 配置key的序列化类
        producer = new Producer<>(new ProducerConfig(props));
        while (true) {
            try {
                T obj = queue.take();
                // 莫得办法这样做，四种数据类型没有一个统一的父类
                if (obj instanceof ThriftObdGps) {
                    ThriftObdGps o = (ThriftObdGps) obj;
                    while (o.getGpstime() > System.currentTimeMillis() / 1000) {
                        Thread.sleep(20);
                    } // to optimize
                    if ("END".equals(o.getSn())) {
                        producer.close();
                        break;
                    }
                    producer.send(new KeyedMessage<>(topic, o.getSn(), ts.serialize(o)));
                } else if (obj instanceof ThriftObdDs) {
                    ThriftObdDs o = (ThriftObdDs) obj;
                    while (o.getGpstime() > System.currentTimeMillis() / 1000) {
                        Thread.sleep(20);
                    } // to optimize
                    if ("END".equals(o.getSn())) {
                        producer.close();
                        break;
                    }
                    producer.send(new KeyedMessage<>(topic, o.getSn(), ts.serialize(o)));
                } else if (obj instanceof ThriftObdEvent) {
                    ThriftObdEvent o = (ThriftObdEvent) obj;
                    while (o.getGpstime() > System.currentTimeMillis() / 1000) {
                        Thread.sleep(20);
                    } // to optimize
                    if ("END".equals(o.getSn())) {
                        producer.close();
                        break;
                    }
                    producer.send(new KeyedMessage<>(topic, o.getSn(), ts.serialize(o)));
                } else if (obj instanceof ThriftObdError) {
                    ThriftObdError o = (ThriftObdError) obj;
                    while (o.getGpstime() > System.currentTimeMillis() / 1000) {
                        Thread.sleep(20);
                    } // to optimize
                    if ("END".equals(o.getSn())) {
                        producer.close();
                        break;
                    }
                    producer.send(new KeyedMessage<>(topic, o.getSn(), ts.serialize(o)));
                } else {
                    System.err.println("DataTypeError: not in (GPS, OBD, ERROR, EVENT)");
                }
            } catch (InterruptedException | TException e) {
                e.printStackTrace();
            }
        }
    }
}
