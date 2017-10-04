package uk.ac.ebi.eva.t2d.utils;

import java.util.HashMap;
import java.util.Map;

public class PropertyUtils {

    /**
     * Flattens a property map to get all the property as single elements instead of a nested property hierarchy
     *
     * @param map
     * @return
     */
    public static Map<String, Object> flatten(Map<String, Object> map) {
        HashMap<String, Object> flattenedMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() instanceof Map<?, ?>) {
                Map<String, Object> flattenedEntries = flatten((Map<String, Object>) entry.getValue());
                for (Map.Entry<String, Object> flattenedEntry : flattenedEntries.entrySet()) {
                    flattenedMap.put(entry.getKey() + "." + flattenedEntry.getKey(), flattenedEntry.getValue());
                }
            } else {
                flattenedMap.put(entry.getKey(), entry.getValue());
            }
        }
        return flattenedMap;
    }

}
