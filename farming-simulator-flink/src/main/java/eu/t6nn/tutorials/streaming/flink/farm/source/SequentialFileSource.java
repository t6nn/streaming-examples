package eu.t6nn.tutorials.streaming.flink.farm.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * @author tonispi
 */
public class SequentialFileSource extends RichSourceFunction<String> {

    private transient BufferedReader reader;
    private volatile boolean running = true;
    private final File inFile;

    public SequentialFileSource(File inFile) {
        this.inFile = inFile;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        reader = new BufferedReader(new FileReader(inFile));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(reader != null) {
            reader.close();
        }
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(running) {
            String line = reader.readLine();
            if(line != null) {
                ctx.collect(line);
            } else {
                Thread.sleep(100);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
