package com.eg.az.eh.nat;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Consumer;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubConsumerAsyncClient;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.messaging.eventhubs.models.PartitionContext;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;

public class ReceiverSimple{

	String connectionString = "";
	String eventHubName = "";

	String storageConnectionString = "";
	String storageContainerName = "";

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		new ReceiverSimple()
			.processEvent();
//			.processEventAsync();
		
	}
	
	BlockingQueue<EventData> queue;
	ThreadPoolExecutor executor;
	
	ReceiverSimple(){
		queue = new ArrayBlockingQueue<>(10);
		executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
		executor.setCorePoolSize(5);
		executor.setMaximumPoolSize(10);
		executor.prestartAllCoreThreads();
	}

//	private void processEventAsync() {
//		// TODO Auto-generated method stub
//		
//		EventHubConsumerAsyncClient consumer = new EventHubClientBuilder()
//				.connectionString(connectionString, eventHubName)
//				.consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
//				.buildAsyncConsumerClient();
//		
////		consumer.receiveFromPartition("0", EventPosition.latest()).subscribe(event -> {
////		    // Process each event as it arrives.
////		});
//		
//		consumer.receive().subscribe(event -> {
//			// Process each event as it arrives.
//
//			PartitionContext partitionContext = event.getPartitionContext();
//			EventData eventData = event.getData();
//			
//			// business logic here
//			//perform(partitionContext, eventData);	// runs directly
//
//			executor.execute(() -> {
//				// do something
//			});
//			
//		});
//
//	}

	private void processEvent() {
		// TODO Auto-generated method stub

		BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
				.connectionString(storageConnectionString).containerName(storageContainerName).buildAsyncClient();

		// Create a builder object that you will use later to build an event processor
		// client to receive and process events and errors.
		EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
				.connectionString(connectionString, eventHubName)
				.consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
				.processEvent(PARTITION_PROCESSOR)
				.processError(ERROR_HANDLER)
				.checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient));

		// Use the builder object to create an event processor client
		EventProcessorClient eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();

		System.out.println("Starting event processor");
		eventProcessorClient.start();

		System.out.println("Press enter to stop.");
		try {
			System.in.read();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Stopping event processor");
		eventProcessorClient.stop();

	}

	public final Consumer<EventContext> PARTITION_PROCESSOR = eventContext -> {

		executor.submit(() -> {

			PartitionContext partitionContext = eventContext.getPartitionContext();
			EventData eventData = eventContext.getEventData();

			System.out.printf("Processing event from partition %s with sequence number %d with body: %s%n",
					partitionContext.getPartitionId(), eventData.getSequenceNumber(), eventData.getBodyAsString());
			
			try {
				Thread.currentThread().sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// Every 10 events received, it will update the checkpoint stored in Azure Blob Storage.
			if (eventData.getSequenceNumber() % 10 == 0) {
				eventContext.updateCheckpoint();
			}		
			
		});

	};

	public final Consumer<ErrorContext> ERROR_HANDLER = errorContext -> {
		System.out.printf("Error occurred in partition processor for partition %s, %s.%n",
				errorContext.getPartitionContext().getPartitionId(), errorContext.getThrowable());
	};

}
