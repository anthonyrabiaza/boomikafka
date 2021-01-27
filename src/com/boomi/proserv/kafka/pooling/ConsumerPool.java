package com.boomi.proserv.kafka.pooling;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.consumer.Consumer;

public class ConsumerPool extends GenericObjectPool<Consumer<String, String>> {

	public ConsumerPool(PooledObjectFactory<Consumer<String, String>> factory) {
		super(factory);
	}

    public ConsumerPool(PooledObjectFactory<Consumer<String, String>> factory, GenericObjectPoolConfig config) {
        super(factory, config);
    }
}
