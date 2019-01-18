package buaa.act.ucar.playback2kfk;

import buaa.act.ucar.hbaseopt.porter.TableMeta;
import buaa.act.ucar.hbaseopt.utils.ConnHelper;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static buaa.act.ucar.hbaseopt.utils.TimeUtil.getTimestamp;

public class Manager {
    public static void main(String[] args) {
        ExecutorService service = Executors.newFixedThreadPool(TableMeta.N_PLAYBACK_TABLE * 2 + 2);
        String startDate = "2016-06-01 20:43:10.000";
        String stopDate = "2016-06-02 00:00:00.000";
        String targetDate = "2019-01-14 20:43:10.000";
        for (int i = 0; i < TableMeta.N_PLAYBACK_TABLE; i++) {
            service.submit(new GPSPlayback("trustcars-playback-" + i, "ThriftObdGps", getTimestamp(startDate),
                    getTimestamp(stopDate), getTimestamp(targetDate)));
            service.submit(new OBDPlayback("trustcars-playback-" + i, "ThriftObdDs", getTimestamp(startDate),
                    getTimestamp(stopDate), getTimestamp(targetDate)));
        }
//        service.submit(new EVENTPlayback("events2-playback","ThriftObdEvent", getTimestamp(startDate), getTimestamp(stopDate),
//                getTimestamp(targetDate)));
//        service.submit(new ERRORPlayback("events2-playback","ThriftObdError", getTimestamp(startDate), getTimestamp(stopDate),
//                getTimestamp(targetDate)));
        service.shutdown();

        while (!service.isTerminated()) {
            try {
                service.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        ConnHelper.closeConnection();
        System.out.println("Playback manager service is Terminated!");
    }
}

