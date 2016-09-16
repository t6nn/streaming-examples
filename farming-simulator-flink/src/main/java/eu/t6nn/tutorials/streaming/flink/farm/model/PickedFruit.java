package eu.t6nn.tutorials.streaming.flink.farm.model;

/**
 * @author tonispi
 */
public class PickedFruit {

    private final String farmer;
    private final Fruit fruit;
    private final long timestamp;

    public PickedFruit(String farmer, Fruit fruit, long timestamp) {
        this.farmer = farmer;
        this.fruit = fruit;
        this.timestamp = timestamp;
    }

    public String getFarmer() {
        return farmer;
    }

    public Fruit getFruit() {
        return fruit;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PickedFruit that = (PickedFruit) o;

        if (timestamp != that.timestamp) return false;
        if (!farmer.equals(that.farmer)) return false;
        return fruit == that.fruit;

    }

    @Override
    public int hashCode() {
        int result = farmer.hashCode();
        result = 31 * result + fruit.hashCode();
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }
}
