package cn.batchfile.metrics.collector.config;

import javax.annotation.PostConstruct;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("queue")
public class QueueConfig {

	private MemConfig mem;
	
	@PostConstruct
	public void init() {
		mem = new MemConfig();
		mem.setEvents(4096);
	}

	public MemConfig getMem() {
		return mem;
	}

	public void setMem(MemConfig mem) {
		this.mem = mem;
	}
	
}
