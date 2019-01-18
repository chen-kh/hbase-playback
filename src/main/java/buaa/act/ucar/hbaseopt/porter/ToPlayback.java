package buaa.act.ucar.hbaseopt.porter;

import buaa.act.ucar.hbaseopt.porter.TableMeta.Trustcars;
import buaa.act.ucar.hbaseopt.utils.ConnHelper;
import buaa.act.ucar.hbaseopt.utils.RowKeyDesign;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

import static buaa.act.ucar.hbaseopt.porter.TableMeta.Events2;

/**
 * 从trustcars表中读取GPS，OBD，EVENT三种数据，改变rowkey的设计，插入trustcars-playback表中。
 * rowkey的设计改变见：。。。
 */
public class ToPlayback implements Runnable {
    private final String fromTable;
    private final String toTable;
    private final List<String> devlist;
    private final long timestampStart;
    private final long timestampStop;
    private Connection connection;

    public ToPlayback(String fromTable, String toTable, List<String> devlist, long timestampStart, long timestampStop) {
        this.fromTable = fromTable;
        this.toTable = toTable;
        this.devlist = devlist;
        this.timestampStart = timestampStart;
        this.timestampStop = timestampStop;
        this.connection = ConnHelper.getConnection();
    }

    @Override
    public void run() {
        try (Table scanTable = connection.getTable(TableName.valueOf(fromTable)); Table insertTable =
                connection.getTable(TableName.valueOf(toTable))) {
            switch (fromTable) {
                case "trustcars":
                    // scan and insert gps, obd, obd raw family
                    playback(scanTable, insertTable, Trustcars.CF_G, Trustcars.CF_G_COLUMNS);
                    playback(scanTable, insertTable, Trustcars.CF_O, Trustcars.CF_O_COLUMNS);
                    playback(scanTable, insertTable, Trustcars.CF_R, Trustcars.CF_R_COLUMNS);
                    break;
                case "events2":
                    // scan and insert events and errors family
                    playback(scanTable, insertTable, Events2.CF_EVENTS, Events2.CF_EVENTS_COLUMNS);
                    playback(scanTable, insertTable, Events2.CF_ERRORS, Events2.CF_ERRORS_COLUMNS);
                    break;
                default:
                    System.out.println("FromTableError: no this table to playback");
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void playback(Table scanTable, Table insertTable, byte[] family, byte[][] columns) {
        Scan scan = new Scan();
        for (byte[] col : columns) {
            scan.addColumn(family, col);
        }

        long insertedCount = 0L;
        for (int i = 0; i < devlist.size(); i++) {
            String dev = devlist.get(i);

            // scan 保证时间顺序从早到晚
            scan.setStartRow(RowKeyDesign.generateTrustcarsRowkey(timestampStart, dev));
            scan.setStopRow(RowKeyDesign.generateTrustcarsRowkey(timestampStop, dev));
            scan.setReversed(true);

            try (ResultScanner scanner = scanTable.getScanner(scan)) {
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    String rk = Bytes.toString(result.getRow());
                    long timestamp = Long.MAX_VALUE - Long.valueOf(rk.substring(rk.length() - 19));
                    Put p = new Put(RowKeyDesign.generateTrustcarsPlaybackRowkey(timestamp, dev));
                    for (byte[] col : columns) {
                        byte[] val = result.getValue(family, col);
                        if (val != null) {
                            p.addColumn(family, col, val);
                        }
                    }
                    insertTable.put(p);
                    if (insertedCount % 5000 == 0) {
                        Timestamp time = new Timestamp(timestamp * 1000);
                        System.out.println(String.format("%s: %s, %s: %s inserted, %s (%d/%d)",
                                Thread.currentThread().getName(), time.toString(), toTable, new String(family), dev, i,
                                devlist.size()));
                    }
                    insertedCount++;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

