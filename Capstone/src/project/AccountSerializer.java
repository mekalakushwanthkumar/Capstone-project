package project;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AccountSerializer implements Serializer<Account>{
	private final ObjectMapper mapper= new ObjectMapper();

	@Override
	public void configure(Map<String, ?>configs,boolean isKey) {}

	@Override
	public byte[] serialize(String topic,Account data) {
		if(data==null) {
			return null;
		}
		try {
			return mapper.writeValueAsBytes(data);
		}
		catch(Exception e) {
			throw new RuntimeException("Error serializing Account",e);
		}
	}
	@Override
	public void close() {}

}
