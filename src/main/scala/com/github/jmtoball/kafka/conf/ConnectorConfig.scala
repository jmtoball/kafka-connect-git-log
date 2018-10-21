package com.github.jmtoball.kafka.conf

import java.util

import org.apache.kafka.common.config.AbstractConfig

class ConnectorConfig(config: util.Map[String, _]) extends AbstractConfig(Config.connectorConfigDef, config) {}
