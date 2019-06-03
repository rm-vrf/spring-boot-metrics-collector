//package cn.batchfile.metrics.collector.config;
//
//import java.io.IOException;
//import java.util.List;
//
//import javax.jms.ConnectionFactory;
//import javax.jms.JMSException;
//import javax.jms.Message;
//import javax.jms.Queue;
//import javax.jms.Session;
//import javax.jms.TextMessage;
//import javax.jms.Topic;
//
//import org.apache.activemq.command.ActiveMQQueue;
//import org.apache.activemq.command.ActiveMQTopic;
//import org.apache.commons.lang3.NotImplementedException;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.jms.annotation.EnableJms;
//import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
//import org.springframework.jms.config.JmsListenerContainerFactory;
//import org.springframework.jms.support.converter.MessageConversionException;
//import org.springframework.jms.support.converter.MessageConverter;
//
//import com.fasterxml.jackson.core.JsonParseException;
//import com.fasterxml.jackson.core.type.TypeReference;
//import com.fasterxml.jackson.databind.JsonMappingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//
//import cn.batchfile.metrics.collector.domain.MetricData;
//
//@Configuration
//@EnableJms
//public class ActiveMQConfig {
//
//	@Bean
//    public Topic topic() {
//        return new ActiveMQTopic("inmemory.topic");
//    }
//
//	@Bean
//    public Queue queue() {
//        return new ActiveMQQueue("inmemory.queue");
//    }
//	
//	@Bean
//	public JmsListenerContainerFactory<?> jmsListenerContainerTopic(ConnectionFactory connectionFactory, Topic topic) {
//		DefaultJmsListenerContainerFactory bean = new DefaultJmsListenerContainerFactory();
//		bean.setPubSubDomain(true);
//		bean.setConnectionFactory(connectionFactory);
//		bean.setMessageConverter(new MessageConverter() {
//			private ThreadLocal<ObjectMapper> MAPPER = new ThreadLocal<ObjectMapper>() {
//				protected ObjectMapper initialValue() {
//					return new ObjectMapper();
//				}
//			};
//			
//			@Override
//			public Message toMessage(Object object, Session session) throws JMSException, MessageConversionException {
//				throw new NotImplementedException("MessageConverter.toMessage()");
//			}
//			
//			@Override
//			public Object fromMessage(Message message) throws JMSException, MessageConversionException {
//				String text = ((TextMessage)message).getText();
//				try {
//					List<MetricData> list = MAPPER.get().readValue(text, new TypeReference<List<MetricData>>() {});
//					return list;
//				} catch (JsonParseException e) {
//					throw new RuntimeException("error when parse message", e);
//				} catch (JsonMappingException e) {
//					throw new RuntimeException("error when parse message", e);
//				} catch (IOException e) {
//					throw new RuntimeException("error when parse message", e);
//				}
//			}
//		});
//		
//		return bean;
//	}
//
//}
