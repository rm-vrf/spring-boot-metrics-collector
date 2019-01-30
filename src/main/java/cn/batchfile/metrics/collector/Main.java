package cn.batchfile.metrics.collector;

import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestTemplate;

@SpringBootApplication
public class Main {

	@Bean
	public RestTemplate restTemplate(RestTemplateBuilder builder) {
		return builder.setConnectTimeout(10000).setReadTimeout(10000).build();
	}
	
	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(Main.class);
		
		Map<String, Object> properties = new HashMap<>();
		properties.put("spring.config.name", "application,metrics-collector");
		application.setDefaultProperties(properties);
		
		application.run(args);
	}
	
}
