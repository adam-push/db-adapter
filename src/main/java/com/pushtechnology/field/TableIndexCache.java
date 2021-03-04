package com.pushtechnology.field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class TableIndexCache {
    private final Map<String, ArrayList<String>> indexes;

    public TableIndexCache() {
        indexes = new HashMap<>();
    }

    public boolean hasTable(String table) {
        return indexes.containsKey(table);
    }

    public int getIndex(String table, String key) {
        ArrayList<String> keyList = indexes.get(table);
        if(keyList == null) {
            return -1;
        }

        int idx = 0;
        for(String k : keyList) {
            if(k.equals(key)) {
                return idx;
            }
            idx++;
        }

        // Not found
        return -1;
    }

    public int addIndex(String table, String key) {
        ArrayList<String> keyList = indexes.get(table);
        if(keyList == null) {
            keyList = new ArrayList<>();
            indexes.put(table, keyList);
        }

        // Add key to list if it doesn't already exist
        for(String k : keyList) {
            if(k.equals(key)) {
                return -1; // Already exists
            }
        }
        keyList.add(key);
        return keyList.size();
    }

    public int removeIndex(String table, String key) {
        ArrayList<String> keyList = indexes.get(table);
        if(keyList == null) {
            return -1;
        }

        int idx = 0;
        String oldValue = null;
        for(String k : keyList) {
            if(k.equals(key)) {
                oldValue = keyList.remove(idx);
                break;
            }
            idx++;
        }

        if(oldValue != null) {
            return idx;
        }
        else {
            return -1;
        }
    }
}
