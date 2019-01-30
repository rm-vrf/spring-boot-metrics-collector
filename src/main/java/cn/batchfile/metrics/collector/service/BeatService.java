package cn.batchfile.metrics.collector.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.batchfile.metrics.collector.config.BeatConfig;
import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.domain.RawData.Type;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class BeatService {
	
	private static final Logger LOG = LoggerFactory.getLogger(BeatService.class);
	private static Map<String, Function<StringBuilder, List<RawData>>> DRIVERS = new HashMap<String, Function<StringBuilder, List<RawData>>>();
	private static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		};
	};
	private Counter beatCounter;
	private Counter errorCounter;
	private Counter inQueueCounter;
	private Timer beatTimer;
	private long lastRun = 0;

	@Autowired
	private BeatConfig beatConfig;
	
	@Autowired
	private QueueService queueService;
	
	@Autowired
	private RestTemplate restTemplate;
	
	static {
		DRIVERS.put("/metrics", BeatService::parseSpringBootRawData);
		DRIVERS.put("/actuator/prometheus", BeatService::parsePrometheusRawData);
	}
	
	public BeatService(MeterRegistry registry) {
		beatCounter = Counter.builder("beat.ok.count").register(registry);
		errorCounter = Counter.builder("beat.error.count").register(registry);
		inQueueCounter = Counter.builder("in.queue.count").register(registry);
		beatTimer = Timer.builder("beat.time").register(registry);
	}
	
	@PostConstruct
	public void init() {
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
			beat();
		}, 1, 1, TimeUnit.SECONDS);
	}
	
	private void beat() {
		long now = System.currentTimeMillis();
		if (beatConfig.getHosts() != null && now - lastRun >= beatConfig.getPeriod() * 1000) {
			beatConfig.getHosts().parallelStream().forEach(host -> {
				try {
					beat(host);
				} catch (Exception e) {
					LOG.error("error when get metrics, host: " + host, e);
				}
			});
			lastRun = now;
		}
	}
	
	private void beat(String host) throws URISyntaxException {
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
		
		Function<StringBuilder, List<RawData>> function = DRIVERS.get(uri.getPath());
		if (function != null) {
			List<RawData> datas = function.apply(content);
			LOG.debug("parse data, size: {}", datas.size());
			datas.forEach(data -> {
				data.setHost(uri.getHost());
				data.setPort(uri.getPort());
				data.setTime(time.getTime());
			});
			
			if (datas.size() > 0) {
				queueService.put(datas);
				inQueueCounter.increment(datas.size());
			}
		}
	}
	
	private static List<RawData> parseSpringBootRawData(StringBuilder content) {
		Map<String, Double> map = null;
		if (content.length() > 0) {
			try {
				map = MAPPER.get().readValue(content.toString(), new TypeReference<Map<String, Double>>() {});
			} catch (Exception e) {
				LOG.error("error when deserialize content", e);
			}
		}
		
		List<RawData> list = new ArrayList<>();
		if (map != null) {
			for (Entry<String, Double> entry : map.entrySet()) {
				RawData data = new RawData();
				data.setName(entry.getKey());
				data.setValue1(entry.getValue());
				data.setType(data.getName().startsWith("counter.") ? Type.COUNTER : Type.GAUGE);
				list.add(data);
			}
		}
		
		return list;
	}
	
	private static List<RawData> parsePrometheusRawData(StringBuilder content) {
		List<RawData> list = new ArrayList<>();
		return list;
	}
	
}
