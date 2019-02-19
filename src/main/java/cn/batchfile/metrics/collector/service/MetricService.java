package cn.batchfile.metrics.collector.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import cn.batchfile.metrics.collector.domain.MetricData;
import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.domain.RawData.Type;
import cn.batchfile.metrics.collector.functions.CounterMetricsComposer;
import cn.batchfile.metrics.collector.functions.DefaultMetricsComposer;
import cn.batchfile.metrics.collector.functions.SummaryMetricsComposer;

@Service
public class MetricService {
	protected static final Logger LOG = LoggerFactory.getLogger(MetricService.class);
	private Map<Type, Function<RawData, List<MetricData>>> COMPOSERS = new HashMap<Type, Function<RawData, List<MetricData>>>();
	
	@Autowired
	private CacheService cacheService;

	@PostConstruct
	public void init() {
		COMPOSERS.put(Type.COUNTER, new CounterMetricsComposer(cacheService));
		COMPOSERS.put(Type.GAUGE, new DefaultMetricsComposer());
		COMPOSERS.put(Type.HISTOGRAM, new DefaultMetricsComposer());
		COMPOSERS.put(Type.SUMMARY, new SummaryMetricsComposer(cacheService));
		COMPOSERS.put(Type.UNTYPED, new DefaultMetricsComposer());
	}

	public List<MetricData> compose(RawData data) {
		
		Type type = data.getType();
		Function<RawData, List<MetricData>> function = COMPOSERS.get(type);
		if (function != null) {
			return function.apply(data);
		} else {
			return null;
		}
	}

}