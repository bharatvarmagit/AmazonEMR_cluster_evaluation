package com.group9.application.project;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

public class MapUtility {

	public static Map<String, Integer> buildKeywordTotalMap(Iterable<Text> values) {

		// Creates a map with a counter for detected overall scores

		Map<String, Integer> map = new HashMap<String, Integer>();
		for (Text component : values) {
			String element = component.toString();
			if (!map.containsKey(element)) {
				map.put(element, 1);
			} else {
				map.put(element, map.get(element) + 1);
			}
		}
		return map;
	}
}
