package com.boomi.proserv.kafka.pooling;

import java.util.Date;
import java.util.Properties;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerFactory extends BasePooledObjectFactory<Consumer<String, String>> {
	
	private Properties properties;
	private long timeout = 5 * 60 * 1000;
	
	public long getTimeout() {
		return timeout;
	}

	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public Consumer<String, String> create() throws Exception {
		Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		return consumer;
	}

	@Override
	public PooledObject<Consumer<String, String>> wrap(Consumer<String, String> consumer) {
		return new DefaultPooledObject<Consumer<String, String>>(consumer);
	}

	@Override
	public void passivateObject(PooledObject<Consumer<String, String>> consumer) throws Exception {
		super.passivateObject(consumer);
	}

	@Override
	public boolean validateObject(PooledObject<Consumer<String, String>> consumer) {
		return (new Date().getTime() - consumer.getLastReturnTime()) < timeout;
	}
}
