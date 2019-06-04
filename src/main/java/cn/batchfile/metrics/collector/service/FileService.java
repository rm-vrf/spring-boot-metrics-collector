package cn.batchfile.metrics.collector.service;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import cn.batchfile.metrics.collector.config.FileConfig;
import cn.batchfile.metrics.collector.domain.MetricData;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Service
public class FileService {
	private static final Logger LOG = LoggerFactory.getLogger(FileService.class);
	private static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		};
	};
	
	private Counter writeCounter;
	private Counter errorCounter;
	
	@Autowired
	private QueueService queueService;
	
	@Autowired
	private FileConfig fileConfig;

	public FileService(MeterRegistry registry) {
		writeCounter = Counter.builder("file.write.ok.count").register(registry);
		errorCounter = Counter.builder("file.write.error.count").register(registry);
	}
	
	@PostConstruct
	public void init() {
		if (!fileConfig.isEnabled()) {
			LOG.info("FILE OUTPUT DISABLED");
			return;
		}
		
		Logger writer = createWriter();
		
		queueService.consume((MetricData metric) -> {
			try {
				String content = MAPPER.get().writeValueAsString(metric);
				writer.info(content);
				writeCounter.increment();
			} catch (Exception e) {
				errorCounter.increment();
				LOG.error("error when consumer", e);
			}
		});
	}

	@SuppressWarnings("unchecked")
	private ch.qos.logback.classic.Logger createWriter() {
		String name = "metrics-collector";
		
		LoggerContext logCtx = (LoggerContext)LoggerFactory.getILoggerFactory();
		
		PatternLayoutEncoder logEncoder = new PatternLayoutEncoder();
		logEncoder.setContext(logCtx);
		logEncoder.setPattern("%msg%n");
		logEncoder.start();
		
		@SuppressWarnings("rawtypes")
		RollingFileAppender logFileAppender = new RollingFileAppender();
		logFileAppender.setContext(logCtx);
		logFileAppender.setName(name);
		logFileAppender.setEncoder(logEncoder);
		logFileAppender.setAppend(true);
		logFileAppender.setFile(fileConfig.getName());
		
		@SuppressWarnings("rawtypes")
		TimeBasedRollingPolicy logFilePolicy = new TimeBasedRollingPolicy();
		logFilePolicy.setContext(logCtx);
		logFilePolicy.setParent(logFileAppender);
		logFilePolicy.setFileNamePattern(fileConfig.getName() + ".%d{" + fileConfig.getFileNamePattern() + "}.txt");
		logFilePolicy.setMaxHistory(fileConfig.getMaxHistory());
		logFilePolicy.start();
		
		logFileAppender.setRollingPolicy(logFilePolicy);
		logFileAppender.start();
		
		ch.qos.logback.classic.Logger log = logCtx.getLogger(name);
		log.setAdditive(false);
		log.setLevel(Level.INFO);
		log.addAppender(logFileAppender);
		
		return log;
	}

}
