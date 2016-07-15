package eu.t6nn.tutorials.streaming.flink.farm;

import eu.t6nn.tutorials.streaming.flink.farm.job.StreamBuilder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Bootstrapper {

	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			throw new IllegalArgumentException("Usage: Job num_farmers start_port");
		}

		int farmerCount = Integer.valueOf(args[0]);
		int startPort = Integer.valueOf(args[1]);

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamBuilder streamBuilder = new StreamBuilder(farmerCount, startPort);

		streamBuilder.build(environment);
		environment.execute();
	}
}
