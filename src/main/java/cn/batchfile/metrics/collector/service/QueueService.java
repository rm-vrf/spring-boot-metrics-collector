package cn.batchfile.metrics.collector.service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.batchfile.metrics.collector.config.QueueConfig;
import cn.batchfile.metrics.collector.domain.MetricData;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

@Service
public class QueueService {
	private static final Logger LOG = LoggerFactory.getLogger(QueueService.class);
	private List<BlockingQueue<MetricData>> queues = new ArrayList<>();
	private boolean run = true;
	
	@Autowired
	private QueueConfig queueConfig;
	
	public QueueService(MeterRegistry registry) {
		Gauge.builder("queue.size", StringUtils.EMPTY, (s) -> {
			Iterator<BlockingQueue<MetricData>> iter = queues.iterator();
			long i = 0;
			while (iter.hasNext()) {
				i += iter.next().size();
			}
			return i;
		}).register(registry);
	}
	
	@PreDestroy
	public void destroy() {
		run = false;
	}
	
	public void put(MetricData metric) throws InterruptedException {
		Iterator<BlockingQueue<MetricData>> iter = queues.iterator();
		while (iter.hasNext()) {
			BlockingQueue<MetricData> queue = iter.next();
			if (!queue.offer(metric, 1, TimeUnit.SECONDS)) {
				LOG.warn("Access queue memory limit!");
			}
		}
	}
	
	public void consume(Consumer consumer) {
		BlockingQueue<MetricData> queue = new LinkedBlockingQueue<>(queueConfig.getMem().getEvents());
		queues.add(queue);
		new Thread(() -> {
			while (run) {
				try {
					MetricData metric = queue.poll(1, TimeUnit.SECONDS);
					if (metric != null) {
						consumer.consume(metric);
					}
				} catch (Exception e) {
					LOG.error("error when consumer data " + consumer.toString(), e);
				}
			}
		}).start();
	}
	
}
