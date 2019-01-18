package buaa.act.ucar.hbaseopt.utils;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {
    public static long getTimestamp(String date) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
            Date parsedDate = dateFormat.parse(date);
            Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
            return timestamp.getTime() / 1000;
        } catch (Exception e) { //this generic but you can control another types of exception
            e.printStackTrace();
        }
        return -1L;
    }
}
