package eu.t6nn.tutorials.streaming.spark.farm.job;

import eu.t6nn.tutorials.streaming.flink.farm.model.Fruit;
import eu.t6nn.tutorials.streaming.flink.farm.model.FruitCount;
import eu.t6nn.tutorials.streaming.flink.farm.model.PickedFruit;
import eu.t6nn.tutorials.streaming.spark.farm.source.SequentialFileReceiver;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.io.File;
import java.util.List;

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

    public void build(JavaStreamingContext sc) {
        JavaDStream<PickedFruit> fruitStream = pickedFruitStream( sourceStream( sc, 1 ), "farmer1" );
        for ( int idx = 2; idx <= farmerCount; idx++ ) {
            fruitStream = fruitStream.union( pickedFruitStream( sourceStream( sc, idx ), "farmer" + idx ) );
        }
        fullCrateCount(fruitStream, 50L).print();
    }

    private JavaDStream<String> sourceStream(JavaStreamingContext sc, int sourceIndex ) {
        File inFile = new File( storageDirectory, "farmer" + sourceIndex + ".log" );
        return sc.receiverStream(new SequentialFileReceiver(inFile));
    }

    private JavaDStream<PickedFruit> pickedFruitStream( JavaDStream<String> source, final String farmer ) {
        return source.map( row -> {
            String[] cols = StringUtils.split( row );
            if ( cols.length != 2 ) {
                throw new IllegalArgumentException( "Must have two input columns." );
            }
            return new PickedFruit( farmer, Fruit.fromString( cols[0] ), Long.valueOf( cols[1] ) );
        } );
    }

    private JavaDStream<Tuple2<Fruit, Long>> fullCrateCount(JavaDStream<PickedFruit> fruitStream, final long crateSize) {
        return fruitStream
                .mapToPair(pf -> new Tuple2<>(pf.getFruit(), new FruitCount(pf)))
                .updateStateByKey((List<FruitCount> fruitCounts, Optional<FruitCount> state) -> {
                    FruitCount newCount = state.orElse(FruitCount.ZERO);
                    for(FruitCount inCount : fruitCounts) {
                        newCount = newCount.add(inCount);
                    }
                    return Optional.of(newCount);
                })
                .map(count -> new Tuple2<>(count._1(), count._2().getCount() / crateSize));
    }
}
