package buaa.act.ucar.playback2kfk;

import buaa.act.ucar.hbaseopt.utils.ConnHelper;
import buaa.act.ucar.hbaseopt.utils.RowKeyDesign;
import com.zuche.us.thrift.ThriftObdGps;
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
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static buaa.act.ucar.hbaseopt.porter.TableMeta.Trustcars;

public class GPSPlayback extends Playback<ThriftObdGps> {
    public GPSPlayback(String dataTableName, String topic, long timestampStart, long timestampStop, long timestampTarget) {
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
            String sn; // required
            double lon = -1.0; // optional
            double lat = -1.0; // optional
            int accuracy = -1; // optional
            double speed = -1.0; // optional
            int direction = -1; // optional
            double height = -1.0; // optional
            int positionMode = -1; // optional

            // config hbase
            Connection connection = ConnHelper.getConnection();
            try (Table table = connection.getTable(TableName.valueOf(dataTableName))) {
                Scan scan = new Scan();
                for (byte[] col : Trustcars.CF_G_COLUMNS) {
                    scan.addColumn(Trustcars.CF_G, col);
                }

                scan.setStartRow(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestampStart, "999999999999")); //
                // smallest
                scan.setStopRow(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestampStop, "000000000000")); // largest
                scan.setReversed(true);

                ResultScanner scanner = table.getScanner(scan);
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    String rk = Bytes.toString(result.getRow());
                    long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(0, 19));
                    sn = rk.substring(rk.length() - 12);
                    for (byte[] col : Trustcars.CF_G_COLUMNS) {
                        byte[] val = result.getValue(Trustcars.CF_G, col);
                        if (val != null) {
                            switch (new String(col)) {
                                case "JD":
                                    lon = Bytes.toDouble(val);
                                    break;
                                case "WD":
                                    lat = Bytes.toDouble(val);
                                    break;
                                case "S":
                                    speed = Bytes.toDouble(val);
                                    break;
                                case "D":
                                    direction = Bytes.toInt(val);
                                    break;
                                case "H":
                                    height = Bytes.toDouble(val);
                                    break;
                                case "PM":
                                    positionMode = Bytes.toInt(val);
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                    // create object
                    ThriftObdGps gps = new ThriftObdGps();
                    gps.setSn(sn);
                    gps.setGpstime(timestamp + (timestampTarget - timestampStart));
                    gps.setSpeed(speed);
                    gps.setAccuracy(accuracy);
                    gps.setHeight(height);
                    gps.setLat(lat);
                    gps.setLon(lon);
                    gps.setDirection(direction);
                    gps.setPositionMode(positionMode);

                    // serialize and produce
                    if (gps.getSn() != null) {
                        queue.put(gps);
                    }
                }
                scanner.close();
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    ThriftObdGps end = new ThriftObdGps();
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
            long count = 0L;
            while (true) {
                try {
                    ThriftObdGps gps = queue.take();
                    while (gps.getGpstime() > System.currentTimeMillis() / 1000) {
                        Thread.sleep(20);
                    } // to optimize
                    if ("END".equals(gps.getSn())) {
                        producer.close();
                        break;
                    }
                    producer.send(new KeyedMessage<>(topic, gps.getSn(), ts.serialize(gps)));
                    if (count % 200 == 0 && dataTableName.equals("trustcars-playback-0"))
                        System.out.println(Thread.currentThread().getName() + ", gps: " + new Timestamp(gps.getGpstime() * 1000) + ", now: " + new Timestamp(System.currentTimeMillis()));
                    count++;
                } catch (InterruptedException | TException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
