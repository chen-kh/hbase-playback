package buaa.act.ucar.hbaseopt.porter;

import org.apache.hadoop.hbase.util.Bytes;

public class TableMeta {
    public static int N_PLAYBACK_TABLE = 16;
    public static class Trustcars {
        public static String NAME = "trustcars";
        public static byte[][] FAMILIES = new byte[][]{Bytes.toBytes("G"), Bytes.toBytes("O"), Bytes.toBytes("R")};
        public static byte[][] CF_G_COLUMNS = new byte[][]{Bytes.toBytes("D"), Bytes.toBytes("S"), Bytes.toBytes("JD"),
                Bytes.toBytes("WD"), Bytes.toBytes("A"), Bytes.toBytes("PM")};
        public static byte[][] CF_O_COLUMNS = new byte[][]{Bytes.toBytes("S"), Bytes.toBytes("ES"), Bytes.toBytes("M"),
                Bytes.toBytes("TF"), Bytes.toBytes("TM"), Bytes.toBytes("V")};
        public static byte[][] CF_R_COLUMNS = new byte[][]{Bytes.toBytes("R")};

        public static byte[] CF_G = Bytes.toBytes("G");
        public static byte[] CF_O = Bytes.toBytes("O");
        public static byte[] CF_R = Bytes.toBytes("R");
        public static class G {
            public static final byte[] DIRECTION = Bytes.toBytes("D");
            public static final byte[] SPEED = Bytes.toBytes("S");
            public static final byte[] LONGITUDE = Bytes.toBytes("JD");
            public static final byte[] LATITUDE = Bytes.toBytes("WD");
            public static final byte[] ACCURACY = Bytes.toBytes("A");
            public static final byte[] HEIGHT = Bytes.toBytes("H");
            public static final byte[] POSITION_MODE = Bytes.toBytes("PM");
        }

        public static class O {
            public static final byte[] OBD_SPEED = Bytes.toBytes("S");
            public static final byte[] ENGINE_SPEED = Bytes.toBytes("ES");
            public static final byte[] MILEAGE = Bytes.toBytes("M");
            public static final byte[] TOTAL_FUEL = Bytes.toBytes("TF");
            public static final byte[] TOTAL_MILEAGE = Bytes.toBytes("TM");
            public static final byte[] VIN = Bytes.toBytes("V");
        }

        public static class R {
            public static final byte[] RAW = Bytes.toBytes("R");
        }
    }

    public static class TrustcarsPlayback {
        public static String NAME = "trustcars-playback";
        public static byte[][] FAMILIES = new byte[][]{Bytes.toBytes("G"), Bytes.toBytes("O"), Bytes.toBytes("R")};
        public static byte[][] CF_G_COLUMNS = new byte[][]{Bytes.toBytes("D"), Bytes.toBytes("S"), Bytes.toBytes("JD"),
                Bytes.toBytes("WD"), Bytes.toBytes("A"), Bytes.toBytes("PM")};
        public static byte[][] CF_O_COLUMNS = new byte[][]{Bytes.toBytes("S"), Bytes.toBytes("ES"), Bytes.toBytes("M"),
                Bytes.toBytes("TF"), Bytes.toBytes("TM"), Bytes.toBytes("V")};
        public static byte[][] CF_R_COLUMNS = new byte[][]{Bytes.toBytes("R")};

        public static byte[] CF_G = Bytes.toBytes("G");
        public static byte[] CF_O = Bytes.toBytes("O");
        public static byte[] CF_R = Bytes.toBytes("R");

        public static class G {
            public static final byte[] DIRECTION = Bytes.toBytes("D");
            public static final byte[] SPEED = Bytes.toBytes("S");
            public static final byte[] LONGITUDE = Bytes.toBytes("JD");
            public static final byte[] LATITUDE = Bytes.toBytes("WD");
            public static final byte[] ACCURACY = Bytes.toBytes("A");
            public static final byte[] HEIGHT = Bytes.toBytes("H");
            public static final byte[] POSITION_MODE = Bytes.toBytes("PM");
        }

        public static class O {
            public static final byte[] OBD_SPEED = Bytes.toBytes("S");
            public static final byte[] ENGINE_SPEED = Bytes.toBytes("ES");
            public static final byte[] MILEAGE = Bytes.toBytes("M");
            public static final byte[] TOTAL_FUEL = Bytes.toBytes("TF");
            public static final byte[] TOTAL_MILEAGE = Bytes.toBytes("TM");
            public static final byte[] VIN = Bytes.toBytes("V");
        }

        public static class R {
            public static final byte[] RAW = Bytes.toBytes("R");
        }
    }

    public static class Events2{
        public static byte[] NAME = Bytes.toBytes("events2");
        public static byte[][] FAMILIES = new byte[][]{Bytes.toBytes("EVENTS"), Bytes.toBytes("ERRORS")};
        public static byte[][] CF_EVENTS_COLUMNS = new byte[][]{Bytes.toBytes("E"), Bytes.toBytes("LO"), Bytes.toBytes("LA"),
                Bytes.toBytes("T"), Bytes.toBytes("OV"), Bytes.toBytes("NV")};
        public static byte[][] CF_ERRORS_COLUMNS = new byte[][]{Bytes.toBytes("FC"), Bytes.toBytes("FCS")};

        public static byte[] CF_EVENTS = Bytes.toBytes("EVENTS");
        public static byte[] CF_ERRORS = Bytes.toBytes("ERRORS");

        public static class EVENTS{
            public static final byte[] EVENT_TIME = Bytes.toBytes("E");
            public static final byte[] LONGITUDE = Bytes.toBytes("LO");
            public static final byte[] LATITUDE = Bytes.toBytes("LA");
            public static final byte[] TYPE_ID = Bytes.toBytes("T");
            public static final byte[] OLD_VIN = Bytes.toBytes("OV");
            public static final byte[] NEW_VIN = Bytes.toBytes("NV");

        }
        static class ERRORS{
            public static final byte[] FAULT_CODE = Bytes.toBytes("FC");
            public static final byte[] FAULT_CODE_STATE = Bytes.toBytes("FCS");
        }
    }
}
