package cn.batchfile.metrics.collector.service;

import cn.batchfile.metrics.collector.domain.MetricData;

@FunctionalInterface
public interface Consumer {

	void consume(MetricData metric);

}
