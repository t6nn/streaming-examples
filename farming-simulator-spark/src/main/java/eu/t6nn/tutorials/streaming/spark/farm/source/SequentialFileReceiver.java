package eu.t6nn.tutorials.streaming.spark.farm.source;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.*;

/**
 * @author tonispi
 */
public class SequentialFileReceiver extends Receiver<String> {

    private transient BufferedReader reader;
    private volatile boolean running = true;
    private final File inFile;

    public SequentialFileReceiver(File inFile) {
        super(StorageLevel.DISK_ONLY());
        this.inFile = inFile;
    }


    @Override
    public void onStart() {
        try {
            reader = new BufferedReader(new FileReader(inFile));
            new Thread(new ReaderThread()).start();
        } catch (FileNotFoundException e) {
        }
    }

    @Override
    public void onStop() {
        running = false;
    }

    private boolean readAndStore() throws IOException {
        final String line = reader.readLine();
        if(line != null) {
            store(line);
        }
        return line != null;
    }

    private class ReaderThread implements Runnable {
        @Override
        public void run() {
            while(running) {
                try {
                    if(!readAndStore()) {
                        Thread.sleep(100);
                    }
                } catch (IOException | InterruptedException ignored) {
                    return;
                }
            }
        }

    }
}
