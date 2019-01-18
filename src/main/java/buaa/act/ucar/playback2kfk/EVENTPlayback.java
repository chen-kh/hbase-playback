package buaa.act.ucar.playback2kfk;

import buaa.act.ucar.hbaseopt.porter.TableMeta.Events2;
import buaa.act.ucar.hbaseopt.utils.ConnHelper;
import buaa.act.ucar.hbaseopt.utils.RowKeyDesign;
import com.zuche.us.thrift.ThriftObdEvent;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EVENTPlayback extends Playback<ThriftObdEvent> {

    public EVENTPlayback(String dataTableName, String topic, long timestampStart, long timestampStop,
                         long timestampTarget) {
        super(dataTableName, topic, timestampStart, timestampStop, timestampTarget);
    }

    @Override
    public void run() {
        ExecutorService service = Executors.newFixedThreadPool(2);
        service.submit(new PlaybackScanner());
        service.submit(new PlaybackProducer());
//        service.submit(new buaa.act.ucar.playback2kfk.MyPlaybackProducer<ThriftObdGps>(topic, queue));
        service.shutdown();
        while (!service.isTerminated()) {
            try {
                service.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class PlaybackScanner implements Runnable {
        @Override
        public void run() {
            // value to produce
            String sn = null;
            long gpstime = -1L;
            long timestamp = -1L;
            long eventtime = -1L;
            int type = -1;
            double lon = -1.0;
            double lat = -1.0;
            String oldVin = null;
            String newVin = null;
            String res = null;

            // config hbase
            Connection connection = ConnHelper.getConnection();
            try (Table table = connection.getTable(TableName.valueOf(dataTableName))){
                Scan scan = new Scan();
                for (byte[] col : Events2.CF_EVENTS_COLUMNS) {
                    scan.addColumn(Events2.CF_EVENTS, col);
                }

                scan.setStartRow(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestampStart, "999999999999")); // smallest
                scan.setStopRow(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestampStop, "000000000000")); // largest
                scan.setReversed(true);

                ResultScanner scanner = table.getScanner(scan);
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    String rk = Bytes.toString(result.getRow());
                    timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(0, 19));
                    gpstime = timestamp;
                    sn = rk.substring(rk.length() - 12);
                    for (byte[] col : Events2.CF_EVENTS_COLUMNS) {
                        byte[] val = result.getValue(Events2.CF_EVENTS, col);
                        if (val != null) {
                            switch (new String(col)) {
                                case "E":
                                    eventtime = Bytes.toLong(val);
                                    break;
                                case "LO":
                                    lon = Bytes.toDouble(val);
                                    break;
                                case "LA":
                                    lat = Bytes.toDouble(val);
                                    break;
                                case "T":
                                    type = Bytes.toInt(val);
                                    break;
                                case "OV":
                                    oldVin = Bytes.toString(val);
                                    break;
                                case "NV":
                                    newVin = Bytes.toString(val);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }

                    // create object ???? there isn't res
                    ThriftObdEvent event = new ThriftObdEvent();
                    event.setEventtime(eventtime);
                    event.setLon(lon);
                    event.setLat(lat);
                    event.setGpstime(gpstime);
                    event.setSn(sn);
                    event.setType(type);
                    event.setOldVin(oldVin);
                    event.setNewVin(newVin);

                    // serialize and produce
                    if (event.getSn() != null) {
                        queue.put(event);
                    }
                }
                scanner.close();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    ThriftObdEvent end = new ThriftObdEvent();
                    end.setSn("END");
                    queue.put(end);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class PlaybackProducer implements Runnable {

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
                    ThriftObdEvent o = queue.take();
                    while (o.getGpstime() > System.currentTimeMillis() / 1000) {
                        Thread.sleep(20);
                    } // to optimize
                    if ("END".equals(o.getSn())) {
                        producer.close();
                        break;
                    }
                    producer.send(new KeyedMessage<>(topic, o.getSn(), ts.serialize(o)));
                } catch (InterruptedException | TException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
