package cn.batchfile.metrics.collector.service;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;

import cn.batchfile.metrics.collector.domain.RawData;

@Service
public class CacheService {
	protected static final Logger LOG = LoggerFactory.getLogger(CacheService.class);
	private Map<String, RawData> datas = new ConcurrentHashMap<>(1024 * 1024);
	private static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		};
	};

	@PostConstruct
	public void init() throws IOException {
		//load counter from file
		String home = System.getProperty("user.home");
		File file = new File(new File(home), ".spring-boot-metrics-collector-counter");
		if (file.exists()) {
			List<String> lines = FileUtils.readLines(file, "UTF-8");
			for (String line : lines) {
				try {
					RawData data = MAPPER.get().readValue(line, RawData.class);
					datas.put(getKey(data), data);
				} catch (Exception e) {
					LOG.error("error when read data from tmp file", e);
				}
			}
		}

		//save data every 10s
		Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
			StringBuilder s = new StringBuilder();
			List<String> removes = new ArrayList<>();
			long now = System.currentTimeMillis();
			int count = 0;
			for (Entry<String, RawData> entry : datas.entrySet()) {
				if (now - entry.getValue().getTime() < 1800000) {
					try {
						s.append(MAPPER.get().writeValueAsString(entry.getValue())).append("\n");
						count ++;
					} catch (Exception e) {
						LOG.error("error when write json", e);
					}
				} else {
					removes.add(entry.getKey());
				}
			}
			
			try {
				if (count > 0) {
					FileUtils.writeByteArrayToFile(file, s.toString().getBytes("UTF-8"));
				}
			} catch (UnsupportedEncodingException e) {
				LOG.error("error when write file", e);
			} catch (IOException e) {
				LOG.error("error when write file", e);
			}
			
			for (String key : removes) {
				datas.remove(key);
			}
			
		}, 10, 10, TimeUnit.SECONDS);
	}
	
	public void put(RawData data) {
		datas.put(getKey(data), data);
	}
	
	public RawData get(RawData data) {
		return datas.get(getKey(data));
	}
	
	private String getKey(RawData data) {
		return String.format("%s_%s_%s_%s", data.getHost(), data.getPort(), data.getName(), data.getTags());
	}

}
