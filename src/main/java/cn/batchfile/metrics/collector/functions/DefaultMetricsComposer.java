package cn.batchfile.metrics.collector.functions;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.function.Function;

import cn.batchfile.metrics.collector.domain.MetricData;
import cn.batchfile.metrics.collector.domain.RawData;

public class DefaultMetricsComposer implements Function<RawData, List<MetricData>> {

	public static ThreadLocal<SimpleDateFormat> FORMATTER = new ThreadLocal<SimpleDateFormat>() {
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
		};
	};
	
	@Override
	public List<MetricData> apply(RawData data) {
		MetricData metric = new MetricData();
		metric.setMetric(data.getName());
		metric.setTimestamp(FORMATTER.get().format(new Date(data.getTime())));
		metric.setValue(data.getValues()[0]);
		metric.getTags().put("host", data.getHost());
		metric.getTags().put("port", String.valueOf(data.getPort()));
		if (data.getTags() != null) {
			metric.getTags().putAll(data.getTags());
		}

		List<MetricData> metrics = new ArrayList<>();
		metrics.add(metric);
		return metrics;
	}

}
