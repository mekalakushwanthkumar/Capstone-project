package project;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class AccountTypePartitioner implements Partitioner{
	@Override
	public void configure(Map<String,?> configs) {}

	@Override
	public int partition(String topic,Object keyObj,byte[] keyBytes,Object valueObj,byte[] valueBytes,Cluster cluster) {
		String accountType=null;

		if(keyObj instanceof String) {
			accountType=(String) keyObj;
		} else if (valueObj instanceof Account) {
			accountType=((Account) valueObj).getAccountType();
		}
		else if (valueObj instanceof String) {
			accountType=extractAccountTypeFromJsonString((String) valueObj);
		}

		if(accountType==null) {
			accountType="UNKNOWN";
		}
		accountType=accountType.toUpperCase();
		int partitionCount=cluster.partitionCountForTopic(topic);

		switch (accountType) {
		case "CA": return 0 %partitionCount;
		case "SB": return 1 %partitionCount;
		case "RD": return 2 %partitionCount;
		case "LOAN": return 3 %partitionCount;
		default:
			return Math.abs(accountType.hashCode())%Math.max(partitionCount,1);
		}
	}
	
	private String extractAccountTypeFromJsonString(String s) {
		if(s==null) {
			return null;
		}
		String token="\"accountType\"";
		int idx=s.indexOf(token);
		if(idx==-1) {
			return null;
		}
		int colon=s.indexOf(":",idx);
		if(colon==-1) {
			return null;
		}
		int firstQuote=s.indexOf('"',colon);
		if(firstQuote==-1) {
			return null;
		}
		int secondQuote=s.indexOf('"',colon);
		if(secondQuote==-1) {
			return null;
		}
		return s.substring(firstQuote+1,secondQuote);
	}

	
	@Override
	public void close() {}


}