package org.janusgraph.core.util;

import com.google.common.collect.HashMultimap;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Profiler {

    private static Map<String, Long> times = new HashMap<>();

    private static HashMultimap<String, String> children = HashMultimap.create();

    public static void updateTime(String key, long increment) {
        times.merge(key, increment, Long::sum);
    }

    public static void updateFromCurrentTime(String key, long start) {
        times.merge(key, System.currentTimeMillis() - start, Long::sum);
    }

    public static void updateChildFromCurrentTime(String parentKey, String childKey, long start) {
        times.merge(childKey, System.currentTimeMillis() - start, Long::sum);
        children.put(parentKey, childKey);
    }

    private static void printFamily(String key, int level){
        Set<String> dependants = children.get(key);
        Long val = times.get(key);
        if ( val != null) System.out.println(indent(level) + new AbstractMap.SimpleEntry<>(key, val));
        dependants.stream()
                .sorted(Comparator.comparing(dep -> -times.get(dep)))
                .forEach(dep -> printFamily(dep, level +1));
    }

    public static void printTimes(){
        Set allChildren = new HashSet<>(children.values());
        times.keySet().stream()
                .filter(k -> !allChildren.contains(k))
                .sorted(Comparator.comparing(k -> -times.get(k)))
                .forEach(k -> printFamily(k, 0));
    }

    private static String indent(int size){
        char[] charIndent = new char[size];
        Arrays.fill(charIndent, '\t');
        return String.valueOf(charIndent);
    }
}