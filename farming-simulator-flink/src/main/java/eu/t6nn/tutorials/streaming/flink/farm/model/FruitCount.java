package eu.t6nn.tutorials.streaming.flink.farm.model;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * @author tonispi
 */
public class FruitCount {
    private final Fruit fruit;
    private final long count;

    public FruitCount(Fruit fruit) {
        this(fruit, 1);
    }

    private FruitCount(Fruit fruit, long count) {
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
        assert fruit == other.fruit;

        return new FruitCount(fruit, this.count + other.count);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("fruit", fruit)
                .append("count", count)
                .toString();
    }
}
