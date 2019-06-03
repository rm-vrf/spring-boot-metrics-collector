package cn.batchfile.metrics.collector.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("queue")
public class QueueConfig {

	private MemConfig mem;
	
	public MemConfig getMem() {
		return mem;
	}

	public void setMem(MemConfig mem) {
		this.mem = mem;
	}

}
