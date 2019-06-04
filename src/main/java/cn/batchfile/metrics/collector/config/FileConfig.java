package cn.batchfile.metrics.collector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("output.file")
public class FileConfig {

	private boolean enabled = false;
	private String name = "metrics";
	private String fileNamePattern = "yyyy-MM-dd";
	private int maxHistory = 0;
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFileNamePattern() {
		return fileNamePattern;
	}
	
	public void setFileNamePattern(String fileNamePattern) {
		this.fileNamePattern = fileNamePattern;
	}
	
	public int getMaxHistory() {
		return maxHistory;
	}
	
	public void setMaxHistory(int maxHistory) {
		this.maxHistory = maxHistory;
	}

}
