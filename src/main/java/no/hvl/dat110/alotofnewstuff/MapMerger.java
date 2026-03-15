package no.hvl.dat110.alotofnewstuff;

import java.util.Map;

public class MapMerger {
    @SafeVarargs
    public static <K, V> Map<K, V> merge(Map<K, V>... maps) {
        Map<K, V> result = new java.util.HashMap<>();
        for(Map<K, V> map : maps) {
            result.putAll(map);
        }
        return result;
    }
}
