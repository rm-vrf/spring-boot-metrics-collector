package cn.batchfile.metrics.collector.functions;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import cn.batchfile.metrics.collector.domain.MetricData;
import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.service.CacheService;

public class CounterMetricsComposer implements Function<RawData, List<MetricData>> {
	
	private CacheService cacheService;
	
	public CounterMetricsComposer(CacheService cacheService) {
		this.cacheService = cacheService;
	}

	@Override
	public List<MetricData> apply(RawData data) {
		MetricData metric = new MetricData();
		metric.setMetric(data.getName());
		metric.setTimestamp(DefaultMetricsComposer.FORMATTER.get().format(new Date(data.getTime())));
		metric.setValue(data.getValues()[0]);
		metric.getTags().put("host", data.getHost());
		metric.getTags().put("port", String.valueOf(data.getPort()));
		if (data.getTags() != null) {
			metric.getTags().putAll(data.getTags());
		}
		
		RawData oldData = cacheService.get(data);
		if (oldData != null && data.getValues()[0] >= oldData.getValues()[0]) {
			metric.setValue(data.getValues()[0] - oldData.getValues()[0]);
		}
		cacheService.put(data);
		
		List<MetricData> metrics = new ArrayList<>();
		metrics.add(metric);
		return metrics;
	}

}
