package cn.batchfile.metrics.collector.service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
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
	private BlockingQueue<MetricData> queue;
	private Counter writeCounter;
	private Counter errorCounter;
	private Timer writeTimer;
	private boolean run = true;
	
	@Autowired
	private ElasticsearchConfig elasticsearchConfig;
	
	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private QueueService queueService;

	public ElasticsearchService(MeterRegistry registry) {
		writeCounter = Counter.builder("elasticsearch.write.ok.count").register(registry);
		errorCounter = Counter.builder("elasticsearch.write.error.count").register(registry);
		writeTimer = Timer.builder("elasticsearch.write.time").register(registry);
		Gauge.builder("elasticsearch.write.queue.size", StringUtils.EMPTY, 
				(s) -> queue == null ? 0 : queue.size()).register(registry);
	}
	
	@PostConstruct
	public void init() {
		if (!elasticsearchConfig.isEnabled()) {
			LOG.info("ELASTICSEARCH OUTPUT DISABLED");
			return;
		}
		
		int capacity = 100 * elasticsearchConfig.getWorker() * elasticsearchConfig.getBulkMaxSize() * elasticsearchConfig.getMaxRetries();
		queue = new LinkedBlockingQueue<>(capacity);
		LOG.info("init elasticsearch writer, capacity: {}, workers: {}", capacity, elasticsearchConfig.getWorker());
		
		Executors.newFixedThreadPool(elasticsearchConfig.getWorker()).submit(() -> {
			while (run) {
				try {
					consumer();
				} catch (Exception e) {
					LOG.error("error when consumer", e);
				}
			}
		});
		
		queueService.consume((MetricData metric) -> {
			try {
				queue.offer(metric, 1, TimeUnit.SECONDS);
			} catch (Exception e) {
				LOG.error("error when consumer", e);
			}
		});
	}
	
	@PreDestroy
	public void destroy() {
		run = false;
	}
	
	private void consumer() throws InterruptedException {
		List<MetricData> list = new ArrayList<>();
		while (list.size() < elasticsearchConfig.getBulkMaxSize()) {
			MetricData data = queue.poll(1, TimeUnit.SECONDS);
			if (data == null) {
				break;
			} else if (!elasticsearchConfig.isOmitZero() || data.getValue() != 0) {
				list.add(data);
			}
		}
		
		if (list.size() > 0) {
			final String indexName = new SimpleDateFormat(elasticsearchConfig.getIndex()).format(new Date());
			List<String> hosts = elasticsearchConfig.getHosts();
			String host = hosts.size() == 1 ? hosts.get(0) : hosts.get(new Random().nextInt(hosts.size()));
			write(list, indexName, host);
		}
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
			int i = 0;
			while (i < elasticsearchConfig.getMaxRetries()) {
				try {
					bulk(url, body);
					LOG.info("write data to elasticsearch, size: {}", metrics.size());
					writeCounter.increment();
					break;
				} catch (Exception e) {
					if (++i >= elasticsearchConfig.getMaxRetries()) {
						LOG.error("error when write elasticsearch", e);
						errorCounter.increment();
						//throw new RuntimeException("error when write elasticsearch", e);
						break;
					}
				}
			}
		});
	}
	
	private void bulk(String url, String body) {
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
		if (!status.is2xxSuccessful()) {
			throw new RuntimeException(status.value() + ", " + status.getReasonPhrase());
		}
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
