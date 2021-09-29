package br.com.ecommerce;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonDeserializer<T> implements Deserializer<T>{
	
	public static final String TYPE_CONFIG = "TYPE_CONFIG";
	private final Gson gson = new GsonBuilder().create();
	private Class<T> type;
	
	@SuppressWarnings("unchecked")
	public void configure(Map<String, ?> configs, boolean isKey) {
		String typeName = String.valueOf(configs.get(TYPE_CONFIG));
		try {
			this.type = (Class<T>) Class.forName(typeName);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Class not found", e);
		}
	}

	public T deserialize(String s, byte[] bytes) {
		return gson.fromJson(new String(bytes), this.type);
	}

}