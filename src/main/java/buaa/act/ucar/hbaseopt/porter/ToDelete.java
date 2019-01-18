package buaa.act.ucar.hbaseopt.porter;

import buaa.act.ucar.hbaseopt.porter.TableMeta.Trustcars;
import buaa.act.ucar.hbaseopt.porter.TableMeta.TrustcarsPlayback;
import buaa.act.ucar.hbaseopt.utils.ConnHelper;
import buaa.act.ucar.hbaseopt.utils.RowKeyDesign;
import buaa.act.ucar.hbaseopt.utils.TimeUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

/**
 * 从trustcars表中读取GPS，OBD，EVENT三种数据，改变rowkey的设计，插入trustcars-playback表中。
 * rowkey的设计改变见：。。。
 */
public class ToDelete implements Runnable {
    private List<String> devlist;
    private long timestampStart;
    private long timestampStop;
    private Connection connection;

    public ToDelete(List<String> devlist, long timestampStart, long timestampStop) {
        this.devlist = devlist;
        this.timestampStart = timestampStart;
        this.timestampStop = timestampStop;
        this.connection = ConnHelper.getConnection();
    }

    @Override
    public void run() {
        ResultScanner scanner = null;
        Table scanTable = null;
        Table deleteTable = null;
        try {
            scanTable = connection.getTable(TableName.valueOf("trustcars"));
            deleteTable = connection.getTable(TableName.valueOf("trustcars"));

            // scan and insert gps family
            Scan scan = new Scan();
            for (byte[] col : Trustcars.CF_G_COLUMNS) {
                scan.addColumn(Trustcars.CF_G, col);
            }

            for (String dev : devlist) {
                scan.setStartRow(RowKeyDesign.generateTrustcarsRowkey(timestampStop, dev));
                scan.setStopRow(RowKeyDesign.generateTrustcarsRowkey(timestampStart, dev));

                scanner = scanTable.getScanner(scan);
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    String rk = Bytes.toString(result.getRow());
                    long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19));
                    Delete delete = new Delete(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestamp, dev));
                    delete.addFamily(TrustcarsPlayback.CF_G);
                    Get g = new Get(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestamp, dev));
                    boolean rowExists = deleteTable.exists(g);
                    Timestamp time = new Timestamp(timestamp * 1000);
                    if(!rowExists) {
                        System.out.println(Thread.currentThread().getName() + ": " + time.toString() + " gps is null");
                        if(timestamp < TimeUtil.getTimestamp("2016-06-20 00:00:00.000"))
                            break;
                    }else{
                        deleteTable.delete(delete);
                        System.out.println(Thread.currentThread().getName() + ": " + time.toString() + " gps deleted");
                    }

                }
            }

            // scan and insert obd family
            scan = new Scan();
            for (byte[] col : Trustcars.CF_O_COLUMNS) {
                scan.addColumn(Trustcars.CF_O, col);
            }

            for (String dev : devlist) {
                scan.setStartRow(RowKeyDesign.generateTrustcarsRowkey(timestampStop, dev));
                scan.setStopRow(RowKeyDesign.generateTrustcarsRowkey(timestampStart, dev));

                scanner = scanTable.getScanner(scan);
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    String rk = Bytes.toString(result.getRow());
                    long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19));
                    Delete delete = new Delete(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestamp, dev));
                    delete.addFamily(TrustcarsPlayback.CF_O);
                    Get g = new Get(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestamp, dev));
                    boolean rowExists = deleteTable.exists(g);
                    Timestamp time = new Timestamp(timestamp * 1000);
                    if(!rowExists) {
                        System.out.println(Thread.currentThread().getName() + ": " + time.toString() + " obd is null");
                        if(timestamp < TimeUtil.getTimestamp("2016-06-20 00:00:00.000"))
                            break;
                    }else{
                        deleteTable.delete(delete);
                        System.out.println(Thread.currentThread().getName() + ": " + time.toString() + " obd deleted");
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (scanner != null)
                scanner.close();
            if (scanTable != null) {
                try {
                    scanTable.close();
                    deleteTable.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}

