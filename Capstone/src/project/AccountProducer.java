package project;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class AccountProducer {
	public static void main(String[] args) throws Exception{

		

		Properties props=new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AccountSerializer.class);
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AccountTypePartitioner.class);

		Producer<String,Account> producer=new KafkaProducer<>(props);
		String topic="accounts-topic";
		
		
		for (int i = 0; i < 100; i++) {
            Account account = new Account();
            account.setAccountNumber(i);
            account.setCustomerId(i + 1000);
            account.setAccountType(i % 4 == 0 ? "CA" : i % 4 == 1 ? "SB" : i % 4 == 2 ? "RD" : "LOAN");
            account.setBranch("Branch-" + (i % 10));

            producer.send(new ProducerRecord<>(topic, account.getAccountType(), account));
        }
		System.out.println("Meassage sent.");

		/*Account[] sample=new Account[]{
			new Account(10003,501,"CA","hebbal"),
			new Account(10004,502,"SB","hebbal"),
			new Account(10005,503,"RD","ulsoor"),
			new Account(10006,504,"LOAN","panjakutta"),
			new Account(10007,505,"CA","ulsoor"),
		};
		for(Account a:sample) {
			ProducerRecord<String,Account> record=new ProducerRecord<>(topic, a.getAccountType(), a);
			RecordMetadata md=producer.send(record).get();
			System.out.printf("Sent %s to partition=%d offset=%d%n",a,md.partition(),md.offset());
		}*/
	}
}
