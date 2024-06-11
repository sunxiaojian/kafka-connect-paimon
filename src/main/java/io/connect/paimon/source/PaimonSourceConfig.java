package io.connect.paimon.source;

import org.apache.kafka.common.config.AbstractConfig;

import java.util.Map;

/**
 * Paimon source config
 */
public class PaimonSourceConfig extends AbstractConfig {
    public PaimonSourceConfig(Map<?, ?> originals) {
        super(null, originals);
    }
}
