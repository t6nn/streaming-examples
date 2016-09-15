package eu.t6nn.tutorials.streaming.flink.farm;

import eu.t6nn.tutorials.streaming.flink.farm.job.StreamBuilder;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;

public class Bootstrapper {

	public static void main(String[] args) throws Exception {

		if(args.length != 2) {
			throw new IllegalArgumentException("Usage: Job num_farmers directory");
		}

		int farmerCount = Integer.valueOf(args[0]);
		File storageDirectory = new File(args[1]);
		assert storageDirectory.isDirectory();

		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamBuilder streamBuilder = new StreamBuilder(farmerCount, storageDirectory);

		streamBuilder.build(environment);
		environment.execute();
	}
}
