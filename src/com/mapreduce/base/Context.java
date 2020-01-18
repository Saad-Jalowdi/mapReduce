package com.mapreduce.base;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;

public class Context<K, V> implements Serializable, Writeable<K, V> {
    private SortedMap<K, LinkedList<V>> map ;

    public Context() {
        map = new TreeMap<>();
    }

    public Context(SortedMap<K, LinkedList<V>> map) {
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
