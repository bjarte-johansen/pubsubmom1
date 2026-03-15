package no.hvl.dat110.alotofnewstuff;

import java.util.HashMap;
import java.util.Map;


/*
 * we dont need this but we were horsing around and wanted to see if we could make a fluent map builder
 * it is not type safe and relies on the caller to provide correct key-value pairs but it is a fun way to build
 * maps in a fluent style
 */

@Deprecated
public class FluentMap<K, V> extends HashMap<K, V> {
    public FluentMap() {
        super();
    }

    /*
    public V put(K k, V v) {
        return super.put(k, v);
    }

    public void putAll(Map<? extends K, ? extends V> m) {
        super.putAll(m);
    }
     */

    @SuppressWarnings("unchecked")
    public FluentMap<K, V> with(Object ...keyValuePairs) {
        for(int i = 0; i < keyValuePairs.length; i += 2) {
            K key = (K) keyValuePairs[i];
            V value = (V) keyValuePairs[i + 1];
            this.put(key, value);
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public FluentMap<K, V> with(Map.Entry<K, V>... entries) {
        for(Map.Entry<K, V> entry : entries) {
            this.put(entry.getKey(), entry.getValue());
        }
        return this;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> FluentMap<K, V> of(Object ...keyValuePairs) {
        if (keyValuePairs.length % 2 != 0) {
            throw new IllegalArgumentException("Key-value pairs must be in pairs");
        }
        FluentMap<K, V> map = new FluentMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            K key = (K) keyValuePairs[i];
            V value = (V) keyValuePairs[i + 1];
            map.put(key, value);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> FluentMap<K, V> of(Map.Entry<K, V>... entries) {
        FluentMap<K, V> map = new FluentMap<>();
        for (Map.Entry<K, V> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}
