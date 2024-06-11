package io.connect.paimon.sink;

import org.apache.kafka.common.config.AbstractConfig;

import java.util.Map;


/**
 * Paimon sink config
 */
public class PaimonSinkConfig extends AbstractConfig {

    public PaimonSinkConfig(Map<?, ?> originals) {
        super(null, originals);
    }
}
