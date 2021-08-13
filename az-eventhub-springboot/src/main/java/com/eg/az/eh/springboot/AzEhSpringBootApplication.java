package com.eg.az.eh.springboot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;

import com.azure.spring.integration.core.EventHubHeaders;
import com.azure.spring.integration.core.api.reactor.Checkpointer;

import java.util.function.Consumer;

import static com.azure.spring.integration.core.AzureHeaders.CHECKPOINTER;

@SpringBootApplication
public class AzEhSpringBootApplication {

	public static final Logger LOGGER = LoggerFactory.getLogger(AzEhSpringBootApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(AzEhSpringBootApplication.class, args);
	}

	@Bean
	public Consumer<Message<String>> consume() {
		return message -> {
			Checkpointer checkpointer = (Checkpointer) message.getHeaders().get(CHECKPOINTER);
			LOGGER.info(
					"New message received: '{}', partition key: {}, sequence number: {}, offset: {}, enqueued time: {}",
					message.getPayload(), message.getHeaders().get(EventHubHeaders.PARTITION_KEY),
					message.getHeaders().get(EventHubHeaders.SEQUENCE_NUMBER),
					message.getHeaders().get(EventHubHeaders.OFFSET),
					message.getHeaders().get(EventHubHeaders.ENQUEUED_TIME));
			checkpointer.success()
					.doOnSuccess(success -> {
						LOGGER.info("Message '{}' successfully checkpointed", message.getPayload());
						onSuccess(message);
					})
					.doOnError(error -> {
						LOGGER.error("Exception found", error);
					}).subscribe();
		};
	}

	private Object onSuccess(Message<String> message) {
		// TODO Auto-generated method stub
		//	Do something on success
		
		//	business logic here
		
		try {
			Thread.currentThread().sleep(8000);	//	for test, delays by business logic
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		LOGGER.info("Done");
		
		return true;
	}

	private Object onError(Throwable error) {
		// TODO Auto-generated method stub
		//	Do something on error
		
		return false;
	}

}
