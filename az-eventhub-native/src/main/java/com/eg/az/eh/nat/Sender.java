package com.eg.az.eh.nat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.google.gson.Gson;

public class Sender {

	private final static int maxBatchCount = 100;

	int count = 10000;

	String connectionString = "";
	String eventHubName = "";

	String path = "data/data2.json";

	int current = 0;

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			new Sender().publishEvents();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void publishEvents() throws IOException {
		// TODO Auto-generated method stub

		BufferedReader br1 = new BufferedReader(new FileReader(path));
		Gson gson = new Gson();
		Map ret1 = gson.fromJson(br1, Map.class);

		EventHubProducerClient producer = new EventHubClientBuilder().connectionString(connectionString, eventHubName)
				.buildProducerClient();

		Iterable<EventData> it = new Iterable<EventData>() {

			@Override
			public Iterator iterator() {
				// TODO Auto-generated method stub

				return new Iterator() {

					int pos = 0;

					@Override
					public boolean hasNext() {
						// TODO Auto-generated method stub
						return (count >= current) && (maxBatchCount > pos);
					}

					@Override
					public EventData next() {
						// TODO Auto-generated method stub

						Map ret2 = ret1;
						ret2.put("last_updated", "" + System.nanoTime());
						ret2.put("seq", "" + current);
						// System.out.println(">>>> LOO1 " + current);
						current++;
						pos++;
						return new EventData(ret2.toString());
					}

				};
			}

		};

		while (current <= count) {
			producer.send(it);
			System.out.println(">>>> LOO2 " + current);
		}

		producer.close();

	}

}
