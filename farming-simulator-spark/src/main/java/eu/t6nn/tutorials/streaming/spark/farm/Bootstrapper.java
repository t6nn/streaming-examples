package eu.t6nn.tutorials.streaming.spark.farm;

import eu.t6nn.tutorials.streaming.spark.farm.job.StreamBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.File;

public class Bootstrapper {

	public static void main( String[] args ) throws InterruptedException {
        if(args.length != 2) {
            throw new IllegalArgumentException("Usage: Bootstrapper num_farmers directory");
        }

        int farmerCount = Integer.valueOf(args[0]);
        File storageDirectory = new File(args[1]);
        assert storageDirectory.isDirectory();

		final String checkpointPath = "spark-checkpoints";

		JavaStreamingContext context = new JavaStreamingContext(createConfiguration(), new Duration(1000));
		context.checkpoint(checkpointPath);

//		JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointPath, () -> {
//			SparkConf conf = createConfiguration();
//			JavaStreamingContext ctx = new JavaStreamingContext( conf, new Duration( 1000 ) );
//			ctx.checkpoint(checkpointPath);
//			return ctx;
//		} );

        new StreamBuilder(farmerCount, storageDirectory).build(context);

        context.start();
        context.awaitTermination();
	}

	private static SparkConf createConfiguration() {
		SparkConf conf = new SparkConf();
		conf.setAppName( "Farming Simulator (Spark)" );
		conf.setMaster( "local[4]" );
		conf.set( "spark.serializer", KryoSerializer.class.getName() );
		conf.set( "spark.streaming.receiver.writeAheadLog.enable", "true" );
		conf.set( "spark.streaming.stopGracefullyOnShutdown", "true" );
		return conf;
	}
}
