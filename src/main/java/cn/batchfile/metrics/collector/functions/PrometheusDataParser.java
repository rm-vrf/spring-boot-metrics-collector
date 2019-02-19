package cn.batchfile.metrics.collector.functions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;

import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.domain.RawData.Type;

public class PrometheusDataParser implements Function<StringBuilder, List<RawData>> {

	@Override
	public List<RawData> apply(StringBuilder content) {
		List<RawData> list = new ArrayList<>();
		final String[] lines = StringUtils.split(StringUtils.remove(content.toString(), '\r'), '\n');
		
		// group by name
		List<List<String>> groups = new ArrayList<>();
		List<String> g = null;
		for (String line : lines) {
			if (StringUtils.startsWith(line, "# HELP ")) {
				if (g != null) {
					groups.add(g);
				}
				g = new ArrayList<>();
			} else {
				g.add(line);
			}
		}
		if (g != null) {
			groups.add(g);
		}
		
		// create data
		for (List<String> group : groups) {
			String[] ary = StringUtils.split(StringUtils.substringAfter(group.get(0), "# TYPE "), " ");
			String name = ary[0];
			Type type = null;
			try {
				type = Type.valueOf(StringUtils.upperCase(ary[1]));
			} catch (Exception e) {
				type = Type.UNTYPED;
			}

			for (int i = 1; i < group.size(); i ++) {
				String t = StringUtils.substringBetween(group.get(i), "{", "}");
				Map<String, String> tags = extractTags(t);
				
				RawData data = new RawData();
				data.setName(name);
				data.setType(type);
				data.setTags(tags);
				
				if (type == Type.SUMMARY) {
					double value1 = Double.valueOf(StringUtils.substringAfterLast(group.get(i), " "));
					i ++;
					double value2 = Double.valueOf(StringUtils.substringAfterLast(group.get(i), " "));
					data.setValues(new double[] {value1, value2});
				} else {
					double value = Double.valueOf(StringUtils.substringAfterLast(group.get(i), " "));
					data.setValues(new double[] {value});
				}
				
				list.add(data);
			}
		}
		
		return list;
	}

	private Map<String, String> extractTags(String s) {
		Map<String, String> map = new HashMap<>();
		String[] ary = StringUtils.split(s, ",");
		if (ary != null) {
			for (String element : ary) {
				if (StringUtils.isNotBlank(element)) {
					String[] kv = StringUtils.split(element, "=");
					if (kv != null && kv.length > 1) {
						map.put(kv[0], StringUtils.replace(kv[1], "\"", ""));
					}
				}
			}
		}
		return map;
	}
}
