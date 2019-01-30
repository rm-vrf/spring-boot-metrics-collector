package cn.batchfile.metrics.collector.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import cn.batchfile.metrics.collector.domain.MetricData;
import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.domain.RawData.Type;

@Service
public class MetricService {
	protected static final Logger LOG = LoggerFactory.getLogger(MetricService.class);
	private static ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		};
	};
	private Map<String, RawData> datas = new ConcurrentHashMap<>(1024 * 1024);
	
	public List<MetricData> compose(RawData rawData) {
		List<MetricData> list = null;
		
		if (rawData.getType() == Type.COUNTER) {
			list = new ArrayList<>();
			MetricData metric = composeCounter(rawData);
			list.add(metric);
		} else if (rawData.getType() == Type.SUMMARY) {
			// TODO
		} else if (rawData.getType() == Type.GAUGE) {
			list = new ArrayList<>();
			MetricData metric = composeGauge(rawData);
			list.add(metric);
		}
		
		return list;
	}
	
	private MetricData composeCounter(RawData rawData) {
		MetricData metric = composeGauge(rawData);
		String key = getKey(rawData);
		RawData oldData = datas.get(key);
		if (oldData != null && rawData.getValue1() >= oldData.getValue1()) {
			metric.setValue(rawData.getValue1() - oldData.getValue1());
		}
		datas.put(key, rawData);
		return metric;
	}
	
	private MetricData composeGauge(RawData rawData) {
		MetricData metric = new MetricData();
		metric.setMetric(rawData.getName());
		metric.setTimestamp(FORMATTER.get().format(new Date(rawData.getTime())));
		metric.setValue(rawData.getValue1());
		metric.getTags().put("host", rawData.getHost());
		metric.getTags().put("port", String.valueOf(rawData.getPort()));
		metric.getTags().putAll(extractTags(rawData.getTags()));
		
		return metric;
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
	
	private String getKey(RawData rawData) {
		return String.format("%s_%s_%s_%s", 
				rawData.getHost(), rawData.getPort(), rawData.getName(), rawData.getTags());
	}

}