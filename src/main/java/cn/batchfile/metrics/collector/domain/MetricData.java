package cn.batchfile.metrics.collector.domain;

import java.util.LinkedHashMap;
import java.util.Map;

public class MetricData {

	private String metric;
	private Map<String, String> tags = new LinkedHashMap<String, String>();
	private String timestamp;
	private double value;

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}
}
