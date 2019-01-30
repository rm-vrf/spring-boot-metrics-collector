package cn.batchfile.metrics.collector.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.batchfile.metrics.collector.config.QueueConfig;
import cn.batchfile.metrics.collector.domain.RawData;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

@Service
public class QueueService {
	
	private ConcurrentLinkedQueue<RawData> memoryQueue = new ConcurrentLinkedQueue<>();
	private AtomicInteger count = new AtomicInteger(0);

	@Autowired
	private QueueConfig queueConfig;
	
	public QueueService(MeterRegistry registry) {
		Gauge.builder("queue.size", StringUtils.EMPTY, s -> count.get()).register(registry);
	}
	
	public List<RawData> get(int max) {
		List<RawData> list = new ArrayList<>();
		RawData data = null;
		while ((data = memoryQueue.poll()) != null) {
			count.decrementAndGet();
			list.add(data);
		}
		
		return list;
	}
	
	public void put(RawData rawData) {
		while (count.get() >= queueConfig.getMem().getEvents()) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				
			}
		}
	
		memoryQueue.add(rawData);
		count.incrementAndGet();
	}
	
	public void put(List<RawData> rawDatas) {
		while (count.get() >= queueConfig.getMem().getEvents()) {
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				
			}
		}
	
		memoryQueue.addAll(rawDatas);
		count.addAndGet(rawDatas.size());
	}
	
	
}
