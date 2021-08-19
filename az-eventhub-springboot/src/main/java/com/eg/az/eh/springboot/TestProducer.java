package com.eg.az.eh.springboot;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.google.gson.Gson;

public class TestProducer {

	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		
		String connectionString = "Endpoint=sb://eh-krc-002.servicebus.windows.net/;SharedAccessKeyName=tester01;SharedAccessKey=J2CvWV3wF7NyswSppmxhHIje1+ppZrUPckGJzwPaYZY=;EntityPath=eh001";
		String eventHubName = "eh001";

		String path = "data/data2.json";
		int count = 1000000;
		int maxCount = 100;
		
		BufferedReader br1 = new BufferedReader(new FileReader(path));
	    Gson gson = new Gson();
	    Map ret1 = gson.fromJson(br1, Map.class);
	    
	    System.out.println(ret1);
	    System.out.println(ret1);

		EventHubProducerClient producer = new EventHubClientBuilder().connectionString(connectionString, eventHubName)
				.buildProducerClient();
		
		Iterable<EventData> it = new Iterable<EventData>() {

			@Override
			public Iterator iterator() {
				// TODO Auto-generated method stub
				return new Iterator() {
					
					int cur = 0;
					
					@Override
					public boolean hasNext() {
						// TODO Auto-generated method stub
						return (count >= cur) && (maxCount >= cur);
					}

					@Override
					public EventData next() {
						// TODO Auto-generated method stub
						
						cur++;
						Map ret2 = ret1; 
						ret2.put("last_updated", ""+System.nanoTime());
						return new EventData(ret2.toString());
					}
					
				};
			}
			
		};
		
		
		System.out.println("Sent "+count);
		
		for (int i = 0; i< 10000; i++) {
			producer.send(it);
			System.out.println(">>>> LOOP" + i);
		}
			
		producer.close();		
		
	}

}
