package eu.t6nn.tutorials.streaming.flink.farm.model;

/**
 * @author tonispi
 */
public enum Fruit {
    APPLE, ORANGE, UNKNOWN;

    public static Fruit fromString(String name) {
        try {
            return Fruit.valueOf(name.toUpperCase());
        } catch(IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
