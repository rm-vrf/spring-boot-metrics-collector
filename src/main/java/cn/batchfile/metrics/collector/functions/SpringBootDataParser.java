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

public class SpringBootDataParser implements Function<StringBuilder, List<RawData>> {

	private static final Logger LOG = LoggerFactory.getLogger(SpringBootDataParser.class);
	private static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		};
	};
	
	@Override
	public List<RawData> apply(StringBuilder content) {
		Map<String, Double> map = null;
		if (content.length() > 0) {
			try {
				map = MAPPER.get().readValue(content.toString(), new TypeReference<Map<String, Double>>() {});
			} catch (Exception e) {
				LOG.error("error when deserialize content", e);
			}
		}
		
		List<RawData> list = new ArrayList<>();
		if (map != null) {
			for (Entry<String, Double> entry : map.entrySet()) {
				RawData data = new RawData();
				data.setName(entry.getKey());
				data.setValues(new double[] {entry.getValue()});
				data.setType(data.getName().startsWith("counter.") ? Type.COUNTER : Type.UNTYPED);
				list.add(data);
			}
		}
		
		return list;
	}

}
