package buaa.act.ucar.hbaseopt.utils;

import org.apache.hadoop.hbase.util.Bytes;

public class RowKeyDesign {
    public static byte[] generateTrustcarsRowkey(long timestamp, String devicesn) {
        StringBuilder sb = new StringBuilder(devicesn.substring(6));
        String prefix = sb.reverse().toString();
        return Bytes.toBytes(prefix + devicesn + Long.toString(Long.MAX_VALUE - timestamp));
    }

    public static byte[] generateTrustcarsPlaybackRowkey(long timestamp, String devicesn) {
        StringBuilder sb = new StringBuilder(devicesn.substring(6));
        String prefix = sb.reverse().toString();
        return Bytes.toBytes(Long.toString(Long.MAX_VALUE - timestamp) + prefix + devicesn);
    }
}
