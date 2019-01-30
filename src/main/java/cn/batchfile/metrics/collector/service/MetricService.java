package cn.batchfile.metrics.collector.service;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import cn.batchfile.metrics.collector.domain.MetricData;
import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.domain.RawData.Type;

@Service
public class MetricService {
	protected static final Logger LOG = LoggerFactory.getLogger(MetricService.class);
	private static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		};
	};
	private static ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		};
	};
	private Map<String, RawData> datas = new ConcurrentHashMap<>(1024 * 1024);

	@PostConstruct
	public void init() throws IOException {
		//load counter from file
		String home = System.getProperty("user.home");
		File file = new File(new File(home), ".spring-boot-metrics-collector-counter");
		if (file.exists()) {
			List<String> lines = FileUtils.readLines(file, "UTF-8");
			for (String line : lines) {
				try {
					RawData data = MAPPER.get().readValue(line, RawData.class);
					datas.put(getKey(data), data);
				} catch (Exception e) {
					LOG.error("error when read data from tmp file", e);
				}
			}
		}
		
		//save data every 10s
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
			StringBuilder s = new StringBuilder();
			List<String> removes = new ArrayList<>();
			long now = System.currentTimeMillis();
			int count = 0;
			for (Entry<String, RawData> entry : datas.entrySet()) {
				if (now - entry.getValue().getTime() < 1800000) {
					try {
						s.append(MAPPER.get().writeValueAsString(entry.getValue())).append("\n");
						count ++;
					} catch (Exception e) {
						LOG.error("error when write json", e);
					}
				} else {
					removes.add(entry.getKey());
				}
			}
			
			try {
				if (count > 0) {
					FileUtils.writeByteArrayToFile(file, s.toString().getBytes("UTF-8"));
				}
			} catch (UnsupportedEncodingException e) {
				LOG.error("error when write file", e);
			} catch (IOException e) {
				LOG.error("error when write file", e);
			}
			
			for (String key : removes) {
				datas.remove(key);
			}
			
		}, 10, 10, TimeUnit.SECONDS);
	}

	public List<MetricData> compose(RawData rawData) {
		List<MetricData> list = null;

		if (rawData.getType() == Type.COUNTER) {
			list = new ArrayList<>();
			MetricData metric = composeCounter(rawData);
			list.add(metric);
		} else if (rawData.getType() == Type.SUMMARY) {
			list = composeSummary(rawData);
		} else if (rawData.getType() == Type.GAUGE) {
			list = new ArrayList<>();
			MetricData metric = composeGauge(rawData);
			list.add(metric);
		}

		return list;
	}

	private List<MetricData> composeSummary(RawData rawData) {
		List<MetricData> list = new ArrayList<>();

		String time = FORMATTER.get().format(new Date(rawData.getTime()));
		String host = rawData.getHost();
		String port = String.valueOf(rawData.getPort());
		Map<String, String> tags = extractTags(rawData.getTags());

		double count = rawData.getValue1();
		double sum = rawData.getValue2();
		double rate = count == 0 ? 0 : sum / count;
		
		String key = getKey(rawData);
		RawData oldData = datas.get(key);
		if (oldData != null && rawData.getValue1() >= oldData.getValue1()) {
			count = rawData.getValue1() - oldData.getValue1();
			sum = rawData.getValue2() - oldData.getValue2();
			rate = count == 0 ? 0 : sum / count;
		}

		MetricData countMetric = new MetricData();
		countMetric.setMetric(rawData.getName() + "_count");
		countMetric.setTimestamp(time);
		countMetric.setValue(count);
		countMetric.getTags().put("host", host);
		countMetric.getTags().put("port", port);
		countMetric.getTags().putAll(tags);
		list.add(countMetric);

		MetricData sumMetric = new MetricData();
		sumMetric.setMetric(rawData.getName() + "_sum");
		sumMetric.setTimestamp(time);
		sumMetric.setValue(sum);
		sumMetric.getTags().put("host", host);
		sumMetric.getTags().put("port", port);
		sumMetric.getTags().putAll(tags);
		list.add(sumMetric);

		MetricData rateMetric = new MetricData();
		rateMetric.setMetric(rawData.getName());
		rateMetric.setTimestamp(time);
		rateMetric.setValue(rate);
		rateMetric.getTags().put("host", host);
		rateMetric.getTags().put("port", port);
		rateMetric.getTags().putAll(tags);
		list.add(rateMetric);

		datas.put(key, rawData);
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
		return String.format("%s_%s_%s_%s", rawData.getHost(), rawData.getPort(), rawData.getName(), rawData.getTags());
	}

}