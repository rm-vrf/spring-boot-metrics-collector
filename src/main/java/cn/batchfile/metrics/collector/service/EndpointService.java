package cn.batchfile.metrics.collector.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import cn.batchfile.metrics.collector.config.BeatConfig;
import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.functions.PrometheusDataParser;
import cn.batchfile.metrics.collector.functions.SpringBootDataParser;
import cn.batchfile.metrics.collector.functions.YammerDataParser;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class EndpointService {
	private static final Logger LOG = LoggerFactory.getLogger(EndpointService.class);
	public Map<String, Function<StringBuilder, List<RawData>>> PARSERS = new LinkedHashMap<String, Function<StringBuilder, List<RawData>>>();
	
	private Timer eurekaTimer;
	private List<String> hosts = new ArrayList<>();
	
	@Autowired
	private BeatConfig beatConfig;
	
	@Autowired
	private RestTemplate restTemplate;
	
	public EndpointService(MeterRegistry registry) {
		eurekaTimer = Timer.builder("eureka.time").register(registry);
		
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
			try {
				refresh();
			} catch (Exception e) {
				LOG.error("error when refresh eureka endpoints", e);
			}
		}, 0, 10, TimeUnit.SECONDS);
	}
	
	@PostConstruct
	public void init() {
		//初始化解析器，顺序很重要，用来寻找指标端点
		PARSERS.put("/prometheus", new PrometheusDataParser());
		PARSERS.put("/yammer/metrics", new YammerDataParser());
		PARSERS.put("/actuator/prometheus", new PrometheusDataParser());
		PARSERS.put("/metrics", new SpringBootDataParser());
	}
	
	public List<String> getHosts() {
		return hosts;
	}
	
	private void refresh() {
		if (StringUtils.isNotEmpty(beatConfig.getEureka())) {
			List<String> hosts = getEurekaHosts(beatConfig.getEureka());
			this.hosts = hosts;
		}
	}
	
	@SuppressWarnings("rawtypes")
	private List<String> getEurekaHosts(String eureka) {
		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		HttpEntity<?> entity = new HttpEntity<>("parameters", headers);
		
		List<String> hosts = new ArrayList<>();
		
		long begin = System.currentTimeMillis();
		ResponseEntity<Map> resp = restTemplate.exchange(eureka, HttpMethod.GET, entity, Map.class);
		eurekaTimer.record(System.currentTimeMillis() - begin, TimeUnit.MILLISECONDS);
		
		@SuppressWarnings("unchecked")
		Map<String, Object> map = resp.getBody();
		lookup(hosts, new StringBuilder(), map);

		return hosts;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void lookup(List<String> hosts, StringBuilder status, Map<String, Object> map) {
		LOG.debug("get map from eureka: {}", map);
		
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
					String url = findMetrics(host);
					if (StringUtils.isNotEmpty(url)) {
						LOG.debug("find metrics endpoint: {}, {}", host, status.toString());
						hosts.add(url);
					}
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
	
	private String findMetrics(String url) {
		try {
			URL urlObject = new URL(url);
			String protocol = urlObject.getProtocol();
			String host = urlObject.getHost();
			int port = urlObject.getPort();
			
			for (String path : PARSERS.keySet()) {
				String s = String.format("%s://%s:%s%s", protocol, host, port, path);
				if (connectionOk(s)) {
					return s;
				}
			}
		} catch (MalformedURLException e) {
			// pass
		}
		
		return null;
	}

	private boolean connectionOk(String url) {
		HttpHeaders headers = new HttpHeaders();
		HttpEntity<?> entity = new HttpEntity<>("parameters", headers);
		try {
			ResponseEntity<String> resp = restTemplate.exchange(url, HttpMethod.HEAD, entity, String.class);
			if (resp.getStatusCode() == HttpStatus.OK) {
				return true;
			}
		} catch (Exception e) {
			//pass
		}
		return false;
	}
	
}
