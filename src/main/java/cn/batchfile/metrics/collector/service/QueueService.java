package cn.batchfile.metrics.collector.service;

import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.Topic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cn.batchfile.metrics.collector.domain.MetricData;

@Service
public class QueueService {
	private static final Logger LOG = LoggerFactory.getLogger(QueueService.class);
	private static ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
		protected ObjectMapper initialValue() {
			return new ObjectMapper();
		}
	};

	@Autowired
    private JmsTemplate jmsTemplate;
	
	@Autowired
    private Topic topic;
	
	public void in(List<MetricData> metrics) {
		jmsTemplate.send(topic, new MessageCreator() {
			@Override
			public Message createMessage(Session session) throws JMSException {
				try {
					String text = MAPPER.get().writeValueAsString(metrics);
					return session.createTextMessage(text);
				} catch (JsonProcessingException e) {
					throw new RuntimeException("error when convert message", e);
				}
			}
		});
		LOG.info("send metrics in queue, size: {}", metrics.size());
	}
}
