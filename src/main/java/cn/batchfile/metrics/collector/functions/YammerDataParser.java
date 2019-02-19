package cn.batchfile.metrics.collector.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.domain.RawData.Type;

public class YammerDataParser implements Function<StringBuilder, List<RawData>> {

	private static final Logger LOG = LoggerFactory.getLogger(YammerDataParser.class);
	private static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		};
	};
	
	@Override
	public List<RawData> apply(StringBuilder content) {
		Map<String, Object> map = null;
		if (content.length() > 0) {
			try {
				map = MAPPER.get().readValue(content.toString(), new TypeReference<Map<String, Object>>() {});
			} catch (Exception e) {
				LOG.error("error when deserialize content", e);
			}
		}
		
		List<RawData> datas = new ArrayList<>();
		if (map != null) {
			parse(datas, map, null, null);
		}
		
		return datas;
	}

	private void parse(List<RawData> datas, Map<String, Object> map, String path, Type type) {
		for (Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			
			if (value instanceof Map<?, ?>) {
				String p = path == null ? key : path + "." + key;
				@SuppressWarnings("unchecked")
				Map<String, Object> m = (Map<String, Object>)value;
				parse(datas, m, p, type);
			} else if (value instanceof String) {
				if (key != null && key.equals("type")) {
					try {
						type = Type.valueOf(((String)value).toUpperCase());
					} catch (Exception e) {
						type = Type.UNTYPED;
					}
				}
			} else if (value instanceof Number) {
				RawData data = new RawData();
				data.setName(path + "." + key);
				data.setValues(new double[] {((Number)value).doubleValue()});
				data.setType(type == null ? Type.UNTYPED : type);
				datas.add(data);
			}
		}
	}
}
