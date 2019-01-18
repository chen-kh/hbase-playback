package buaa.act.ucar.hbaseopt.porter;

import buaa.act.ucar.hbaseopt.utils.ConnHelper;
import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static buaa.act.ucar.hbaseopt.utils.TimeUtil.getTimestamp;

public class PlaybackDataGenerator {
    public static void main(String[] args) {
        List<String> devListAll = getDevList("devlist.txt");
        String startDate = "2016-06-02 00:00:00.000";
        String stopDate = "2016-06-03 00:00:00.000";
        if (devListAll != null && devListAll.size() > 0) {
            ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);
            for (int i = 0; i < devListAll.size(); i++) {
                service.submit(new ToPlayback("events2", "events2-playback", devListAll.subList(i, i + 1),
                        getTimestamp(startDate), getTimestamp(stopDate)));
            }
            for (int i = 0; i < devListAll.size(); i++) {
                service.submit(new ToPlayback("trustcars",
                        "trustcars-playback-" + Math.abs(devListAll.get(i).hashCode() % TableMeta.N_PLAYBACK_TABLE),
                        devListAll.subList(i, i + 1), getTimestamp(startDate), getTimestamp(stopDate)));
            }
            service.shutdown();
            while (!service.isTerminated()) {
                try {
                    service.awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            ConnHelper.closeConnection();
            System.out.println("Done!");
        }
    }

    private static ArrayList<String> getDevList(String filepath) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(filepath)));
            String devlist = reader.readLine();
            return Lists.newArrayList(devlist.split(","));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (reader != null)
                    reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
