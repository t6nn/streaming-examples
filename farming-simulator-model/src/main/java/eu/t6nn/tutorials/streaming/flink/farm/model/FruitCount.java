package eu.t6nn.tutorials.streaming.flink.farm.model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * @author tonispi
 */
public class FruitCount {
    public static final FruitCount ZERO = new FruitCount(Fruit.UNKNOWN, 0L);

    private final Fruit fruit;
    private final long count;
    private long timestamp;

    public FruitCount(PickedFruit pickedFruit) {
        this(pickedFruit.getFruit(), 1);
        this.timestamp = pickedFruit.getTimestamp();
    }

    private FruitCount(Fruit fruit, long count) {
        assert fruit != null;
        assert count >= 0L;
        this.fruit = fruit;
        this.count = count;
    }

    public Fruit getFruit() {
        return fruit;
    }

    public long getCount() {
        return count;
    }

    public FruitCount add(FruitCount other) {
        if(ZERO.equals(this) || ZERO.equals(other)) {
            return ZERO.equals(this) ? other : this;
        }

        assert fruit == other.fruit;

        FruitCount newCount = new FruitCount(fruit, this.count + other.count);
        newCount.timestamp = Math.max(this.timestamp, other.timestamp);
        return newCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FruitCount that = (FruitCount) o;

        if (count != that.count) return false;
        return fruit == that.fruit;

    }

    @Override
    public int hashCode() {
        int result = fruit.hashCode();
        result = 31 * result + (int) (count ^ (count >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("fruit", fruit)
                .append("count", count)
                .append("timestamp", timestamp)
                .toString();
    }
}
