package eu.t6nn.tutorials.streaming.flink.farm.job;

import eu.t6nn.tutorials.streaming.flink.farm.model.PickedFruit;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author tonispi
 */
public class FarmerStreamWatermarksAssigner implements AssignerWithPeriodicWatermarks<PickedFruit> {

    private long lastSeenTimestamp = 0L;

    @Override
    public long extractTimestamp(PickedFruit element, long previousElementTimestamp) {
        long timestamp = element.getTimestamp();
        lastSeenTimestamp = Math.max(timestamp, lastSeenTimestamp);
        return timestamp;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(lastSeenTimestamp);
    }
}
