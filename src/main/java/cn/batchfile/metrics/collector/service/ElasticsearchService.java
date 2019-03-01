package cn.batchfile.metrics.collector.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.batchfile.metrics.collector.config.ElasticsearchConfig;
import cn.batchfile.metrics.collector.domain.MetricData;
import cn.batchfile.metrics.collector.domain.RawData;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Service
public class ElasticsearchService {
	private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchService.class);
	private static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		};
	};
	private Counter writeCounter;
	private Counter errorCounter;
	private Timer writeTimer;
	
	@Autowired
	private ElasticsearchConfig elasticsearchConfig;
	
	@Autowired
	private QueueService queueService;
	
	@Autowired
	private MetricService metricService;
	
	@Autowired
	private RestTemplate restTemplate;

	public ElasticsearchService(MeterRegistry registry) {
		writeCounter = Counter.builder("elasticsearch.write.ok.count").register(registry);
		errorCounter = Counter.builder("elasticsearch.write.error.count").register(registry);
		writeTimer = Timer.builder("elasticsearch.write.time").register(registry);
	}

	@PostConstruct
	public void init() {
		ExecutorService service = Executors.newFixedThreadPool(elasticsearchConfig.getWorker());
		for (int i = 0; i < elasticsearchConfig.getWorker(); i++) {
			service.submit(() ->{
				loop();
			});
		}
	}
	
	private void loop() {
		while (true) {
			try {
				write();
			} catch (Exception e) {
				LOG.error("error in loop", e);
			}
			
			try {
				Thread.sleep(500);
			} catch (Exception e) {
				LOG.error("error when sleep", e);
			}
		}
	}
	
	private void write() {
		List<RawData> datas = queueService.get(elasticsearchConfig.getBulkMaxSize());
		while (datas.size() > 0) {
			write(datas);
			datas = queueService.get(elasticsearchConfig.getBulkMaxSize());
		}
	}
	
	private void write(List<RawData> datas) {
		List<MetricData> metrics = new ArrayList<>();
		datas.forEach(data -> {
			metrics.addAll(metricService.compose(data));
		});
		LOG.debug("metrics, size: {}", metrics.size());
		
		final String indexName = new SimpleDateFormat(elasticsearchConfig.getIndex()).format(new Date());
		List<String> hosts = elasticsearchConfig.getHosts();
		String host = hosts.size() == 1 ? hosts.get(0) : hosts.get(new Random().nextInt(hosts.size()));
		
		if (elasticsearchConfig.isBulk()) {
			writeBulk(metrics, indexName, host);
		} else {
			for (MetricData metric : metrics) {
				writeSingle(metric, indexName, host);
			}
		}
	}
	
	private void writeSingle(MetricData metric, final String indexName, String host) {
		LOG.debug("write metric: {}", metric.getMetric());
		final String url = String.format("http://%s/%s/metric", host, indexName);
		try {
			String body = MAPPER.get().writeValueAsString(metric);
			writeTimer.record(() -> {
				ResponseEntity<String> resp = restTemplate.postForEntity(url, body, String.class);
				String responseBody = resp.getBody();
				LOG.debug(responseBody);
				HttpStatus status = resp.getStatusCode();
				if (status.is2xxSuccessful()) {
					writeCounter.increment();
					LOG.debug("write data to elasticsearch, name: {}", metric.getMetric());
				} else {
					errorCounter.increment();
					LOG.error("elasticsearch error, status: {}, message: {}", status.value(), resp.toString());
				}
			});
		} catch (Exception e) {
			LOG.error("error when write metric, name: " + metric.getMetric(), e);
		}
	}
	
	private void writeBulk(List<MetricData> metrics, final String indexName, String host) {
		List<String> lines = new ArrayList<>();
		for (MetricData metric : metrics) {
			composeLine(lines, metric, indexName, null);
		}
		LOG.debug("line count: {}", lines.size());
		
		final String url = String.format("http://%s/_bulk", host);
		String body = StringUtils.join(lines, "");
		
		writeTimer.record(() -> {
			ResponseEntity<String> resp = restTemplate.postForEntity(url, body, String.class);
			if (LOG.isDebugEnabled()) {
				LOG.debug("response status: {}", resp.getStatusCode().toString());
				String json = resp.getBody();
				try {
					Map<String, Object> map = MAPPER.get().readValue(json, new TypeReference<Map<String, Object>>() {});
					List<?> items = (List<?>)map.get("items");
					LOG.debug("items: {}", items.size());
				} catch (Exception e) {
					//pass
				}
			}
			HttpStatus status = resp.getStatusCode();
			if (status.is2xxSuccessful()) {
				writeCounter.increment();
				LOG.info("write data to elasticsearch, size: {}", metrics.size());
			} else {
				errorCounter.increment();
				LOG.error("elasticsearch error, status: {}, message: {}", status.value(), resp.toString());
			}
		});
	}
	
	private void composeLine(List<String> lines, MetricData metric, String index, String id) {
		String s0 = null;
		if (id == null) {
			s0 = String.format("{\"index\":{\"_index\":\"%s\",\"_type\":\"metric\"}}\n", index);
		} else {
			s0 = String.format("{\"index\":{\"_index\":\"%s\",\"_type\":\"metric\",\"_id\":\"%s\"}}\n", index, id);
		}
		
		String s1 = null;
		try { 
			s1 = MAPPER.get().writeValueAsString(metric) + '\n';
		} catch (Exception e) {
			throw new RuntimeException("error when convert metric data to json", e);
		}
		
		if (s0 != null && s1 != null) {
			lines.add(s0);
			lines.add(s1);
		}
	}
	
}
