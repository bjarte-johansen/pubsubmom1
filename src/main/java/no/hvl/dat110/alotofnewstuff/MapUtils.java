package no.hvl.dat110.alotofnewstuff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class MapUtils {
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> unsafeOf(Object... args) {
        if (args == null || args.length % 2 != 0) {
            throw new IllegalArgumentException("Invalide argument: Key-value pairs must be non-null, and in pairs");
        }

        Map<K, V> map = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            K key = (K) args[i];
            V value = (V) args[i + 1];
            map.put(key, value);
        }
        return map;
    }
}
