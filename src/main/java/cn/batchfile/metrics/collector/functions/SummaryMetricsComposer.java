package cn.batchfile.metrics.collector.functions;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import cn.batchfile.metrics.collector.domain.MetricData;
import cn.batchfile.metrics.collector.domain.RawData;
import cn.batchfile.metrics.collector.service.CacheService;

public class SummaryMetricsComposer implements Function<RawData, List<MetricData>> {

	private CacheService cacheService;
	
	public SummaryMetricsComposer(CacheService cacheService) {
		this.cacheService = cacheService;
	}

	@Override
	public List<MetricData> apply(RawData data) {
		List<MetricData> list = new ArrayList<>();

		String time = DefaultMetricsComposer.FORMATTER.get().format(new Date(data.getTime()));
		String host = data.getHost();
		String port = String.valueOf(data.getPort());

		double count = data.getValues()[0];
		double sum = data.getValues()[1];
		double rate = count == 0 ? 0 : sum / count;
		
		RawData oldData = cacheService.get(data);
		if (oldData != null && data.getValues()[0] >= oldData.getValues()[0]) {
			count = data.getValues()[0] - oldData.getValues()[0];
			sum = data.getValues()[1] - oldData.getValues()[1];
			rate = count == 0 ? 0 : sum / count;
		}

		MetricData countMetric = new MetricData();
		countMetric.setMetric(data.getName() + "_count");
		countMetric.setTimestamp(time);
		countMetric.setValue(count);
		countMetric.getTags().put("host", host);
		countMetric.getTags().put("port", port);
		if (data.getTags() != null) {
			countMetric.getTags().putAll(data.getTags());
		}
		list.add(countMetric);

		MetricData sumMetric = new MetricData();
		sumMetric.setMetric(data.getName() + "_sum");
		sumMetric.setTimestamp(time);
		sumMetric.setValue(sum);
		sumMetric.getTags().put("host", host);
		sumMetric.getTags().put("port", port);
		if (data.getTags() != null) {
			sumMetric.getTags().putAll(data.getTags());
		}
		list.add(sumMetric);

		MetricData rateMetric = new MetricData();
		rateMetric.setMetric(data.getName());
		rateMetric.setTimestamp(time);
		rateMetric.setValue(rate);
		rateMetric.getTags().put("host", host);
		rateMetric.getTags().put("port", port);
		if (data.getTags() != null) {
			rateMetric.getTags().putAll(data.getTags());
		}
		list.add(rateMetric);

		cacheService.put(data);
		return list;
	}
	
}
