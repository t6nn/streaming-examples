package eu.t6nn.tutorials.streaming.flink.farm.function;

import eu.t6nn.tutorials.streaming.flink.farm.model.Fruit;
import eu.t6nn.tutorials.streaming.flink.farm.model.FruitCount;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

/**
 * @author tonispi
 */
public class RunningTotal<T extends Window> extends RichWindowFunction<FruitCount, FruitCount, Fruit, T> {

    ReducingStateDescriptor<FruitCount> countDescriptor = new ReducingStateDescriptor<>(
            "current-total",
            FruitCount::add,
            FruitCount.class
    );

    @Override
    public void apply(Fruit fruit, T window, Iterable<FruitCount> input, Collector<FruitCount> out) throws Exception {
        ReducingState<FruitCount> state = getRuntimeContext().getReducingState(countDescriptor);
        for(FruitCount collected : input) {
            state.add(collected);
        }
        out.collect(state.get());
    }
}
