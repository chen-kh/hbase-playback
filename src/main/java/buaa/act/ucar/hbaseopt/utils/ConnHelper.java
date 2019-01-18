package buaa.act.ucar.hbaseopt.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class ConnHelper {
    private static volatile Connection connection;
//    private Connection connection;
//    private static volatile ConnHelper connHelper;

    //    private ConnHelper() {
//        Configuration conf = HBaseConfiguration.create();
//        conf.set("hbase.zookeeper.quorum", "192.168.10.101");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.setLong("hbase.rpc.timeout", 600000);
//        conf.setLong("hbase.client.scanner.caching", 1000);
//        try {
//            connection = ConnectionFactory.createConnection(conf);
//        } catch (IOException e) {
//            e.printStackTrace(); // should throw connection not created exception
//        }
//    }
//
//    public static ConnHelper getInstance() {
//        if (connHelper == null) {
//            synchronized (ConnHelper.class) {
//                if (connHelper == null) {
//                    connHelper = new ConnHelper();
//                }
//            }
//        }
//        return connHelper;
//    }
//    public Connection getConnection() {
//        return this.connection;
//    }

    private ConnHelper() {

    }

    public static Connection getConnection() {
        if (connection == null) {
            synchronized (ConnHelper.class) {
                if (connection == null) {
                    Configuration conf = HBaseConfiguration.create();
                    conf.set("hbase.zookeeper.quorum", "192.168.10.101");
                    conf.set("hbase.zookeeper.property.clientPort", "2181");
                    conf.setLong("hbase.rpc.timeout", 600000);
                    conf.setLong("hbase.client.scanner.caching", 1000);
                    try {
                        connection = ConnectionFactory.createConnection(conf);
                    } catch (IOException e) {
                        e.printStackTrace(); // should throw connection not created exception
                    }
                }
            }
        }
        return connection;
    }

    public static void closeConnection() {
        if (connection != null && !connection.isClosed()) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
