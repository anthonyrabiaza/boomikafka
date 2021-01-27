package com.boomi.proserv.kafka.pooling;

import java.util.Date;
import java.util.Properties;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class ProducerFactory extends BasePooledObjectFactory<Producer<String, String>> {
	
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
	public Producer<String, String> create() throws Exception {
		Producer<String, String> producer = new KafkaProducer<String, String>(properties);
		return producer;
	}

	@Override
	public PooledObject<Producer<String, String>> wrap(Producer<String, String> producer) {
		return new DefaultPooledObject<Producer<String, String>>(producer);
	}

	@Override
	public void passivateObject(PooledObject<Producer<String, String>> producer) throws Exception {
		super.passivateObject(producer);
	}

	@Override
	public boolean validateObject(PooledObject<Producer<String, String>> producer) {
		return (new Date().getTime() - producer.getLastReturnTime()) < timeout;
	}
}
