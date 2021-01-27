package com.boomi.proserv.kafka.pooling;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.kafka.clients.producer.Producer;

public class ProducerPool extends GenericObjectPool<Producer<String, String>> {

	public ProducerPool(PooledObjectFactory<Producer<String, String>> factory) {
		super(factory);
	}

    public ProducerPool(PooledObjectFactory<Producer<String, String>> factory, GenericObjectPoolConfig config) {
        super(factory, config);
    }
}
