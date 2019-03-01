package cn.batchfile.metrics.collector.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

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
		LOG.debug("get metrics, size: {}", metrics.size());
		
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
		List<String> lines = metrics.stream().map(metric -> composeLine(metric, indexName, null)).collect(Collectors.toList());
		LOG.debug("get lines, size: {}", lines.size());
		
		final String url = String.format("http://%s/_bulk", host);
		String body = StringUtils.join(lines, "\n");
		
		writeTimer.record(() -> {
			ResponseEntity<String> resp = restTemplate.postForEntity(url, body, String.class);
			String responseBody = resp.getBody();
			LOG.debug(responseBody);
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
	
	private String composeLine(MetricData metric, String index, String id) {
		String s = null;
		if (id == null) {
			s = String.format("{\"index\":{\"_index\":\"%s\",\"_type\":\"metric\"}}\n", index);
		} else {
			s = String.format("{\"index\":{\"_index\":\"%s\",\"_type\":\"metric\",\"_id\":\"%s\"}}\n", index, id);
		}
		try { 
			s += MAPPER.get().writeValueAsString(metric);
			return s;
		} catch (Exception e) {
			return null;
		}
	}
	
}
