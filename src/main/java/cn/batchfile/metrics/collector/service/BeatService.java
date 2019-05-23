package cn.batchfile.metrics.collector.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;

import cn.batchfile.metrics.collector.config.BeatConfig;
import cn.batchfile.metrics.collector.domain.MetricData;
import cn.batchfile.metrics.collector.domain.RawData;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class BeatService {
	
	private static final Logger LOG = LoggerFactory.getLogger(BeatService.class);
	private Counter beatCounter;
	private Counter errorCounter;
	private Counter rawDataCounter;
	private Counter inQueueCounter;
	private Timer beatTimer;
	private long lastRun = 0;

	@Autowired
	private BeatConfig beatConfig;
	
	@Autowired
	private QueueService queueService;
	
	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private EndpointService endpointService;
	
	@Autowired
	private MetricService metricService;
	
	public BeatService(MeterRegistry registry) {
		beatCounter = Counter.builder("beat.ok.count").register(registry);
		errorCounter = Counter.builder("beat.error.count").register(registry);
		rawDataCounter = Counter.builder("beat.raw.data.count").register(registry);
		inQueueCounter = Counter.builder("beat.in.queue.count").register(registry);
		beatTimer = Timer.builder("beat.time").register(registry);
	}
	
	@PostConstruct
	public void init() {
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
			try {
				beat();
			} catch (Exception e) {
				LOG.error("error in beat", e);
			}
		}, 10, 1, TimeUnit.SECONDS);
		
	}
	
	private void beat() {
		List<String> hosts = new ArrayList<>();
		if (endpointService.getHosts() != null) {
			hosts.addAll(endpointService.getHosts());
		}
		
		if (beatConfig.getHosts() != null) {
			for (String s : beatConfig.getHosts()) {
				if (!hosts.contains(s)) {
					hosts.add(s);
				}
			}
		}
		
		long now = System.currentTimeMillis();
		if (now - lastRun >= beatConfig.getPeriod() * 1000) {
			hosts.parallelStream().forEach(host -> {
				try {
					beatHost(host);
				} catch (Exception e) {
					LOG.error("error when get metrics, host: " + host, e);
				}
			});
			lastRun = now;
		}
	}
	
	private void beatHost(String host) throws URISyntaxException, JsonProcessingException {
		LOG.debug("get data from host: {}", host);
		URI uri = new URI(host);
		
		Date time = new Date();
		final StringBuilder content = new StringBuilder();
		beatTimer.record(() -> {
			try {
				content.append(restTemplate.getForObject(uri, String.class));
				beatCounter.increment();
			} catch (Exception e) {
				errorCounter.increment();
			}
		});
		LOG.debug("html: {}", content);
		
		Function<StringBuilder, List<RawData>> function = endpointService.PARSERS.get(uri.getPath());
		if (function != null) {
			List<RawData> datas = function.apply(content);
			LOG.info("get data, host: {}, bytes: {}, size: {}", host, content.length(), datas.size());
			
			// 过滤名称
			datas = datas.stream().filter(
					data -> beatConfig.getExcludes() == null || !beatConfig.getExcludes().contains(data.getName())
			).collect(Collectors.toList());
			
			// 添加属性
			datas.forEach(data -> {
				data.setHost(uri.getHost());
				data.setPort(uri.getPort());
				data.setTime(time.getTime());
			});
			rawDataCounter.increment(datas.size());
			
			// 计算指标数据
			if (datas.size() > 0) {
				List<MetricData> metrics = new ArrayList<>(datas.size());
				for (RawData data : datas) {
					List<MetricData> list = metricService.compose(data);
					if (list != null) {
						metrics.addAll(list);
					}
				}
				queueService.in(metrics);
				inQueueCounter.increment(metrics.size());
			}
		}
	}
	
}
