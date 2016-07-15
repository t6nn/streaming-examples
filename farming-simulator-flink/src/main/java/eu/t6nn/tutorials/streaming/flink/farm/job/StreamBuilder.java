package eu.t6nn.tutorials.streaming.flink.farm.job;

import eu.t6nn.tutorials.streaming.flink.farm.model.Fruit;
import eu.t6nn.tutorials.streaming.flink.farm.model.FruitCount;
import eu.t6nn.tutorials.streaming.flink.farm.model.PickedFruit;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;

/**
 * @author tonispi
 */
public class StreamBuilder {

	private static final String HOSTNAME = "localhost";
	private final int farmerCount;
	private final int startPort;

	public StreamBuilder( int farmerCount, int startPort ) {
		assert farmerCount >= 1;
		this.farmerCount = farmerCount;
		this.startPort = startPort;
	}

	public void build( StreamExecutionEnvironment environment ) {
		//DataStream<String> counterSource = sourceStream( environment, 0 );

		DataStream<PickedFruit> fruitStream = pickedFruitStream( sourceStream( environment, 1 ) );
		for ( int idx = 2; idx <= farmerCount; idx++ ) {
			fruitStream = fruitStream.union( pickedFruitStream( sourceStream( environment, idx ) ) );
		}

        DataStream<FruitCount> countStream = fruitCountStream(fruitStream);
        countStream.print();
	}

	private DataStreamSource<String> sourceStream( StreamExecutionEnvironment environment, int sourceIndex ) {
		return environment.socketTextStream( HOSTNAME, startPort + sourceIndex );
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

    private DataStream<FruitCount> fruitCountStream(DataStream<PickedFruit> fruitStream) {
        return fruitStream.map(pf -> new FruitCount(pf.getFruit()))
                .keyBy(FruitCount::getFruit)
                .window(TumblingEventTimeWindows.of(Time.seconds(2)))
                .trigger(EventTimeTrigger.create())
                .reduce(FruitCount::add);
    }

}
