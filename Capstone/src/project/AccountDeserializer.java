package project;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AccountDeserializer implements Deserializer<Account>{
	private final ObjectMapper mapper=new ObjectMapper();

	@Override
	public void configure(Map<String,?> configs,boolean isKey) {}

	@Override
	public Account deserialize(String topic,byte[] data) {
		if(data==null) {
			return null;
		}
		try {
			return mapper.readValue(data, Account.class);
		}
		catch(Exception e) {
			throw new RuntimeException("Error deserializing Account",e);
		}
	}
	@Override
	public void close() {}

}
