package no.hvl.dat110.alotofnewstuff;

import java.util.Map;

public class MapAsAttributesString {
    public static <K, V> String toString(Map<K, V> map){
        return toString(map, true);
    }

    public static <K, V> String toString(Map<K, V> map, boolean nullIsEmpty) {
        String sQuote = "\"";
        String sNull = nullIsEmpty ? "" : "null";
        StringBuilder sb = new StringBuilder();

        for (Map.Entry<K, V> entry : map.entrySet()) {
            sb
                .append(entry.getKey())
                .append("=")
                .append(sQuote)
                .append((entry.getValue() == null) ? sNull : entry.getValue())
                .append(sQuote)
                .append(", ");
        }

        if (!map.isEmpty()) {
            sb.setLength(sb.length() - 2); // Remove the last comma and space
        }

        return sb.toString();
    }
}
