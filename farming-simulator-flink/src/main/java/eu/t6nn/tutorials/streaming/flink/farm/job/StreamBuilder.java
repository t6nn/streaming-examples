package eu.t6nn.tutorials.streaming.flink.farm.job;

import eu.t6nn.tutorials.streaming.flink.farm.function.RunningTotal;
import eu.t6nn.tutorials.streaming.flink.farm.model.Fruit;
import eu.t6nn.tutorials.streaming.flink.farm.model.FruitCount;
import eu.t6nn.tutorials.streaming.flink.farm.model.PickedFruit;
import eu.t6nn.tutorials.streaming.flink.farm.source.SequentialFileSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

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
		DataStream<PickedFruit> fruitStream = pickedFruitStream( sourceStream( environment, 1 ), "farmer1" );
		for ( int idx = 2; idx <= farmerCount; idx++ ) {
			fruitStream = fruitStream.union( pickedFruitStream( sourceStream( environment, idx ), "farmer" + idx ) );
		}
		//DataStream<FruitCount> countStream = fruitCountsPerFiveSeconds( fruitStream );
		DataStream<Tuple2<Fruit, Long>> countStream = fullCrateCount( fruitStream, 50L );
		countStream.print();
	}

	private DataStreamSource<String> sourceStream( StreamExecutionEnvironment environment, int sourceIndex ) {
		File inFile = new File( storageDirectory, "farmer" + sourceIndex + ".log" );

		return environment.addSource(new SequentialFileSource(inFile));
	}

	private DataStream<PickedFruit> pickedFruitStream( DataStreamSource<String> source, final String farmer ) {
		return source.map( row -> {
			String[] cols = StringUtils.split( row );
			if ( cols.length != 2 ) {
				throw new IllegalArgumentException( "Must have two input columns." );
			}
			return new PickedFruit( farmer, Fruit.fromString( cols[0] ), Long.valueOf( cols[1] ) );
		} ).assignTimestampsAndWatermarks( new FarmerStreamWatermarksAssigner() );
	}

	private DataStream<Tuple2<Fruit, Long>> fullCrateCount(DataStream<PickedFruit> fruitStream, long crateSize) {
		return fruitStream
				.map(FruitCount::new)
				.keyBy(FruitCount::getFruit)
				.window(GlobalWindows.create())
				.trigger(PurgingTrigger.of(CountTrigger.of(crateSize)))
				.apply(new RunningTotal<>())
				.map(fc -> new Tuple2<>(fc.getFruit(), fc.getCount() / crateSize));
	}

	private DataStream<FruitCount> fruitCountsPerFiveSeconds(DataStream<PickedFruit> fruitStream ) {
		return fruitStream
				.map(FruitCount::new)
				.keyBy( FruitCount::getFruit )
				.window( TumblingEventTimeWindows.of( Time.seconds( 5 ) ) )
				.trigger( EventTimeTrigger.create() )
				.reduce( FruitCount::add );
	}

}
