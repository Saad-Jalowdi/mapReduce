import java.io.Serializable;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * this class acts as tha Map class provided from java.util.Map
 * the difference is it allows multiple writes on the
 *
 * @param <K> key
 * @param <V> value
 * @author Sa'ad Al Jalowdi.
 */
public class ReducerContext<K, V> implements Serializable, Writeable<K, V> {
    private SortedMap<K, V> map;

    public ReducerContext() {
        map = new TreeMap<>();
    }

    public ReducerContext(SortedMap<K, V> map) {
        this.map = map;
    }

    @Override
    public void write(K k, V v) {
        map.put(k, v);
    }

    public SortedMap<K, V> getMap() {
        return map;
    }

}
