package project;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


public class AccountConsumer {
	public static void main(String[] args) {
		String bootstrapServers="localhost:9092";
		String topic="account-topic";
		String groupId="monitor-group";

		Properties props= new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupId);
		props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
		props.put("value.deserializer",project.AccountDeserializer.class);
		props.put("auto.offset.reset", "earliest");

		KafkaConsumer<String,Account> consumer=new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(topic));

		System.out.println("Consumer Started");
		while(true) {
			ConsumerRecords<String,Account> records=consumer.poll(Duration.ofSeconds(1));
			for(ConsumerRecord<String,Account>r:records) {
				System.out.printf("partition=%d offset=%d key=%s value=%s%n",
						r.partition(),r.offset(),r.key(),r.value());
			}
		}
	}

}
