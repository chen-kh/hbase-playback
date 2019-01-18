package buaa.act.ucar.hbaseopt.admin;

import buaa.act.ucar.hbaseopt.porter.TableMeta;
import buaa.act.ucar.hbaseopt.utils.ConnHelper;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class MyAdmin {
    public static void main(String[] args) {
//        createTrustcarPlaybackTable();
//        createEvents2PlaybackTable();
//        dropTable("trustcars-playback");
        for (int i = 0; i < TableMeta.N_PLAYBACK_TABLE; i++) {
            createTable("trustcars-playback-" + i, "G", "O", "R");
        }
    }

    public static boolean createTable(String tableName, String... families) {
        Connection connection = ConnHelper.getConnection();
        Admin admin = null;
        boolean isAvailable = false;
        try {
            admin = connection.getAdmin();
            //creating table descriptor
            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

            //creating column family descriptor
            for (String f : families) {
                table.addFamily(new HColumnDescriptor(toBytes(f)));
            }

            if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
                admin.createTable(table);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                isAvailable = admin.isTableAvailable(TableName.valueOf(tableName));
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("create table: " + tableName + ", " + (isAvailable ? "SUCC" : "FAIL"));
        return isAvailable;
    }

    public static boolean createTrustcarPlaybackTable() {
        final String tableName = "trustcars-playback";
        final String[] families = new String[]{"G", "O", "R"};
        return createTable(tableName, families);
    }

    public static boolean createEvents2PlaybackTable() {
        final String tableName = "events2-playback";
        final String[] families = new String[]{"EVENTS", "ERRORS"};
        return createTable(tableName, families);
    }

    public static boolean dropTable(String tableName) {
        Connection connection = ConnHelper.getConnection();
        Admin admin = null;
        boolean isAvailable = true;
        try {
            admin = connection.getAdmin();

            if (admin.isTableAvailable(TableName.valueOf(tableName))) {
                // disabling table named emp
                admin.disableTable(TableName.valueOf(tableName));

                // Deleting emp
                admin.deleteTable(TableName.valueOf(tableName));
            }
            isAvailable = admin.isTableAvailable(TableName.valueOf(tableName));

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (admin != null && !admin.isAborted())
                    admin.close();
                if (connection != null && !connection.isClosed())
                    connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("delete table: " + tableName + ", " + (isAvailable ? "FAIL" : "SUCC"));
        return !isAvailable;
    }
}
