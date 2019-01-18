package buaa.act.ucar.playback2kfk;

import java.util.concurrent.LinkedBlockingQueue;

public abstract class Playback<T> implements Runnable {
    String dataTableName;
    String topic;
    long timestampStart;
    long timestampStop;
    long timestampTarget;
    LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<>(10 * 1000);

    public Playback(String dataTableName, String topic, long timestampStart, long timestampStop, long timestampTarget) {
        this.dataTableName = dataTableName;
        this.topic = topic;
        this.timestampStart = timestampStart;
        this.timestampStop = timestampStop;
        this.timestampTarget = timestampTarget;
    }
}