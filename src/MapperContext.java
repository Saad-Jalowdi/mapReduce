import java.io.Serializable;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * this class acts as tha Map class provided from java.util.Map
 * the difference is it allows multiple writes on the same key
 * by adding values into LinkedList<V>.
 * @param <K> key
 * @param <V> value
 * @author Sa'ad Al Jalowdi.
 */
public class MapperContext<K, V> implements Serializable, Writeable<K, V> {
    private SortedMap<K, LinkedList<V>> map ;

    public MapperContext() {
        map = new TreeMap<>();
    }

    public MapperContext(SortedMap<K, LinkedList<V>> map) {
        this.map = map;
    }

    @Override
    public void write(K k, V v) {
        if (map.containsKey(k)) {
            LinkedList<V> tmp = map.get(k);
            tmp.add(v);
            map.put(k, tmp);
        } else {
            LinkedList<V> tmp = new LinkedList<>();
            tmp.add(v);
            map.put(k, tmp);
        }
    }

    public SortedMap<K, LinkedList<V>> getMap(){
        return  map;
    }

}
