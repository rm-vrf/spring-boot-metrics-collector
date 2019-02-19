package cn.batchfile.metrics.collector.domain;

import java.util.Map;

public class RawData {
	
	public enum Type {
		COUNTER,
		GAUGE,
		SUMMARY,
	    HISTOGRAM,
	    UNTYPED,
	}

	private String host;
	private int port;
	private long time;
	private Type type;
	private String name;
	private Map<String, String> tags;
	private double[] values;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, String> getTags() {
		return tags;
	}

	public void setTags(Map<String, String> tags) {
		this.tags = tags;
	}

	public double[] getValues() {
		return values;
	}

	public void setValues(double[] values) {
		this.values = values;
	}

}
