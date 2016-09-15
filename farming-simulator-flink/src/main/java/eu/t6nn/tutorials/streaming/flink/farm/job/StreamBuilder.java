package eu.t6nn.tutorials.streaming.flink.farm.job;

import eu.t6nn.tutorials.streaming.flink.farm.model.Fruit;
import eu.t6nn.tutorials.streaming.flink.farm.model.FruitCount;
import eu.t6nn.tutorials.streaming.flink.farm.model.PickedFruit;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FilePathFilter;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;

import java.io.File;

/**
 * @author tonispi
 */
public class StreamBuilder {

	private final int farmerCount;
	private final File storageDirectory;

	public StreamBuilder( int farmerCount, File storageDirectory ) {
		assert farmerCount >= 1;
		this.farmerCount = farmerCount;
		this.storageDirectory = storageDirectory;
	}

	public void build( StreamExecutionEnvironment environment ) {
		DataStream<PickedFruit> fruitStream = pickedFruitStream( sourceStream( environment, 1 ) );
		for ( int idx = 2; idx <= farmerCount; idx++ ) {
			fruitStream = fruitStream.union( pickedFruitStream( sourceStream( environment, idx ) ) );
		}

		DataStream<FruitCount> countStream = fruitRunningCountWindowStream( fruitStream );
		countStream.print();
	}

	private DataStreamSource<String> sourceStream( StreamExecutionEnvironment environment, int sourceIndex ) {
		File inFile = new File( storageDirectory, "farmer" + sourceIndex + ".log" );
		return environment.readFile( new TextInputFormat( new Path( inFile.toURI() ) ), inFile.toURI().toString(), FileProcessingMode.PROCESS_CONTINUOUSLY, 100, FilePathFilter.createDefaultFilter() );
	}

	private DataStream<PickedFruit> pickedFruitStream( DataStreamSource<String> source ) {
		return source.map( row -> {
			String[] cols = StringUtils.split( row );
			if ( cols.length != 2 ) {
				throw new IllegalArgumentException( "Must have two input columns." );
			}
			return new PickedFruit( Fruit.fromString( cols[0] ), Long.valueOf( cols[1] ) );
		} ).assignTimestampsAndWatermarks( new FarmerStreamWatermarksAssigner() );
	}

	private DataStream<FruitCount> fruitRunningCountWindowStream(DataStream<PickedFruit> fruitStream ) {
		return fruitStream
				.map( pf -> new FruitCount( pf.getFruit() ) )
				.keyBy( FruitCount::getFruit )
				.window( TumblingEventTimeWindows.of( Time.seconds( 5 ) ) )
				.trigger( EventTimeTrigger.create() )
				.reduce( FruitCount::add );
	}

}
