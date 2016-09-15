package eu.t6nn.tutorials.streaming.flink.farm.job;

import eu.t6nn.tutorials.streaming.flink.farm.model.PickedFruit;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author tonispi
 */
public class FarmerStreamWatermarksAssigner implements AssignerWithPunctuatedWatermarks<PickedFruit> {

    @Override
    public Watermark checkAndGetNextWatermark(PickedFruit lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp);
    }

    @Override
    public long extractTimestamp(PickedFruit element, long previousElementTimestamp) {
        return element.getTimestamp();
    }
}
