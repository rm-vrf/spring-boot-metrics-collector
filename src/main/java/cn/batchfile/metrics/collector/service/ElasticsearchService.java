package cn.batchfile.metrics.collector.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.batchfile.metrics.collector.config.ElasticsearchConfig;
import cn.batchfile.metrics.collector.domain.MetricData;
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
	private RestTemplate restTemplate;

	public ElasticsearchService(MeterRegistry registry) {
		writeCounter = Counter.builder("elasticsearch.write.ok.count").register(registry);
		errorCounter = Counter.builder("elasticsearch.write.error.count").register(registry);
		writeTimer = Timer.builder("elasticsearch.write.time").register(registry);
	}
	
	@JmsListener(destination = "inmemory.topic", concurrency = "1", containerFactory = "jmsListenerContainerTopic")
	public void listener(List<MetricData> metrics) throws Exception {
		final String indexName = new SimpleDateFormat(elasticsearchConfig.getIndex()).format(new Date());
		List<String> hosts = elasticsearchConfig.getHosts();
		String host = hosts.size() == 1 ? hosts.get(0) : hosts.get(new Random().nextInt(hosts.size()));
		write(metrics, indexName, host);
	}

	private void write(List<MetricData> metrics, final String indexName, String host) {
		List<String> lines = new ArrayList<>();
		for (MetricData metric : metrics) {
			composeLine(lines, metric, indexName);
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
	
	private void composeLine(List<String> lines, MetricData metric, String index) {
		if (elasticsearchConfig.isOmitZero() && metric.getValue() == 0) {
			return;
		}
		
		String s0 = String.format("{\"index\":{\"_index\":\"%s\",\"_type\":\"metric\"}}\n", index);
		
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
