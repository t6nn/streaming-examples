package eu.t6nn.tutorials.streaming.flink.farm.job;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tonispi
 */
public class StreamBuilder {

    private static final String HOSTNAME = "localhost";
    private final int farmerCount;
    private final int startPort;

    public StreamBuilder(int farmerCount, int startPort) {
        this.farmerCount = farmerCount;
        this.startPort = startPort;
    }

    public void build(StreamExecutionEnvironment environment) {
        DataStream<String> counterSource = environment.socketTextStream(HOSTNAME, startPort);
        List<DataStream<String>> farmerSources = new ArrayList<>();

        for(int i = 1; i <= farmerCount; i++) {
            farmerSources.add(environment.socketTextStream(HOSTNAME, startPort + i));
        }


    }

}