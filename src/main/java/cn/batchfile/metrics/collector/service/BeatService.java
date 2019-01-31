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
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
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
	private Timer eurekaTimer;
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
		eurekaTimer = Timer.builder("eureka.time").register(registry);
	}
	
	@PostConstruct
	public void init() {
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
			beat();
		}, 1, 1, TimeUnit.SECONDS);
		
		if (StringUtils.isNotEmpty(beatConfig.getEureka())) {
			Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
				refresh();
			}, 0, 10, TimeUnit.SECONDS);
		}
	}
	
	private void refresh() {
		List<String> hosts = getEurekaHosts(beatConfig.getEureka());
		beatConfig.setHosts(hosts);
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
		final String[] lines = StringUtils.split(StringUtils.remove(content.toString(), '\r'), '\n');
		
		// group by name
		List<List<String>> groups = new ArrayList<>();
		List<String> g = null;
		for (String line : lines) {
			if (StringUtils.startsWith(line, "# HELP ")) {
				if (g != null) {
					groups.add(g);
				}
				g = new ArrayList<>();
			} else {
				g.add(line);
			}
		}
		if (g != null) {
			groups.add(g);
		}
		
		// create data
		for (List<String> group : groups) {
			String[] ary = StringUtils.split(StringUtils.substringAfter(group.get(0), "# TYPE "), " ");
			String name = ary[0];
			Type type = Type.valueOf(StringUtils.upperCase(ary[1]));

			for (int i = 1; i < group.size(); i ++) {
				RawData data = new RawData();
				data.setName(name);
				data.setType(type);
				data.setTags(StringUtils.substringBetween(group.get(i), "{", "}"));
				Double value1 = Double.valueOf(StringUtils.substringAfterLast(group.get(i), " "));
				data.setValue1(value1);
				if (type == Type.SUMMARY) {
					i ++;
					Double value2 = Double.valueOf(StringUtils.substringAfterLast(group.get(i), " "));
					data.setValue2(value2);
				}
				list.add(data);
			}
		}
		
		return list;
	}
	
	@SuppressWarnings("rawtypes")
	private List<String> getEurekaHosts(String eureka) {
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<?> entity = new HttpEntity<>("parameters", headers);
		
		
		List<String> hosts = new ArrayList<>();
		eurekaTimer.record(() -> {
			ResponseEntity<Map> resp = restTemplate.exchange(eureka, HttpMethod.GET, entity, Map.class);
			@SuppressWarnings("unchecked")
			Map<String, Object> map = resp.getBody();
			lookup(hosts, new StringBuilder(), map);
		});

		return hosts;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void lookup(List<String> hosts, StringBuilder status, Map<String, Object> map) {
		LOG.debug("{}", map);
		
		for (Entry<String, Object> entry : map.entrySet()) {
			if (entry.getValue() instanceof Map) {
				lookup(hosts, status, (Map)entry.getValue());
			} else if (entry.getValue() instanceof List) {
				lookup(hosts, status, (List)entry.getValue());
			} else {
				if (StringUtils.equals(entry.getKey(), "status")) {
					status.delete(0, status.length());
					status.append(entry.getValue());
				} else if (StringUtils.equals(entry.getKey(), "statusPageUrl")) {
					String host = entry.getValue().toString();
					if (StringUtils.endsWith(host, "/actuator/info")) {
						host = StringUtils.replace(host, "/info", "/prometheus");
					} else if (StringUtils.endsWith(host, "/info")) {
						host = StringUtils.replace(host, "/info", "/metrics");
					}
					LOG.debug("{}, {}", status.toString(), host);
					hosts.add(host);
				}
			}
		}
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void lookup(List<String> hosts, StringBuilder status, List<Object> list) {
		LOG.debug("{}", list);
		
		for (Object value : list) {
			if (value instanceof Map) {
				lookup(hosts, status, (Map)value);
			} else if (value instanceof List) {
				lookup(hosts, status, (List)value);
			}
		}
	}

}
