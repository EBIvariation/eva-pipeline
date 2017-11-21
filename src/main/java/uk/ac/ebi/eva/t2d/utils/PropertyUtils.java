/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
