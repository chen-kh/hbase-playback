package buaa.act.ucar.playback2kfk;

import buaa.act.ucar.hbaseopt.utils.ConnHelper;
import buaa.act.ucar.hbaseopt.utils.RowKeyDesign;
import com.zuche.us.thrift.ThriftObdDs;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static buaa.act.ucar.hbaseopt.porter.TableMeta.Trustcars;

public class OBDPlayback extends Playback<ThriftObdDs> {

    private static final String KEY_Speed = "51";
    private static final String KEY_TotalMile = "65";// key="65"对应总油耗
    private static final String KEY_Mile = "40";// key="65"对应mileage
    private static final String KEY_TotalFuel = "42";
    private static final String KEY_EngineSpeed = "52";
    private static final String KEY_Vin = "100";
    private static final String KEY_Devicesn = "devicesn";
    private static final String KEY_Gpstime = "gpstime";

    public OBDPlayback(String dataTableName, String topic, long timestampStart, long timestampStop,
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
            String sn = null; // required
            String vin = null;
            double totalMileage = -1.0;
            double totalFuel = -1.0;
            double mileage = -1.0;
            double speed = -1.0;
            double engineSpeed = -1.0;
            long gpstime = 0L;

            Connection connection = ConnHelper.getConnection();
            try {
                Table table = connection.getTable(TableName.valueOf(dataTableName));
                Scan scan = new Scan();
                scan.addColumn(Trustcars.CF_R, Trustcars.R.RAW);

                scan.setStartRow(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestampStart, "999999999999")); //
                // smallest
                scan.setStopRow(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestampStop, "000000000000")); // largest
                scan.setReversed(true);

                ResultScanner scanner = table.getScanner(scan);
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    String rk = Bytes.toString(result.getRow());
                    long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(0, 19));
                    sn = rk.substring(rk.length() - 12);

                    byte[] val = result.getValue(Trustcars.CF_R, Trustcars.R.RAW);
                    if (val != null) {
                        String raw = new String(val);
                        JSONObject jo = new JSONObject(raw);
                        if (jo.has(KEY_TotalMile))
                            totalMileage = jo.getDouble(KEY_TotalMile);
                        if (jo.has(KEY_Mile))
                            mileage = jo.getDouble(KEY_Mile);
                        if (jo.has(KEY_Speed))
                            speed = jo.getDouble(KEY_Speed);
                        if (jo.has(KEY_EngineSpeed))
                            engineSpeed = jo.getDouble(KEY_EngineSpeed);
                        if (jo.has(KEY_Vin))
                            vin = jo.getString(KEY_Vin);
                        if (jo.has(KEY_Devicesn))
                            sn = jo.getString(KEY_Devicesn);
                        if (jo.has(KEY_Gpstime))
                            gpstime = jo.getLong(KEY_Gpstime);
                        if (jo.has(KEY_TotalFuel))
                            totalFuel = jo.getDouble(KEY_TotalFuel);

                        // create object
                        ThriftObdDs obd = new ThriftObdDs();
                        obd.setSn(sn);
                        obd.setGpstime(gpstime + (timestampTarget - timestampStart));
                        obd.setTotalFuel(totalFuel);
                        obd.setTotalMileage(totalMileage);
                        obd.setMileage(mileage);
                        obd.setSpeed(speed);
                        obd.setEngineSpeed(engineSpeed);
                        obd.setVin(vin);
                        obd.setRes(jo.toString());

                        // serialize and produce
                        if (obd.getSn() != null) {
                            queue.put(obd);
                        }
                    }
                }
                scanner.close();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                ThriftObdDs end = new ThriftObdDs();
                end.setSn("END");
                try {
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
                    ThriftObdDs obd = queue.take();
                    while (obd.getGpstime() > System.currentTimeMillis() / 1000) {
                        Thread.sleep(20);
                    } // to optimize
                    if ("END".equals(obd.getSn())) {
                        producer.close();
                        break;
                    }
                    producer.send(new KeyedMessage<>(topic, obd.getSn(), ts.serialize(obd)));
                    if (count % 100 == 0 && dataTableName.equals("trustcars-playback-0"))
                        System.out.println(Thread.currentThread().getName() + ", obd: " + new Timestamp(obd.getGpstime() * 1000) + ", now: " + new Timestamp(System.currentTimeMillis()));
                    count++;
                } catch (InterruptedException | TException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
