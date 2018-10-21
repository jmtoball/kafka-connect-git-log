package com.github.jmtoball.kafka.conf

import java.util

import org.apache.kafka.common.config.AbstractConfig

class TaskConfig(config: util.Map[String, _]) extends AbstractConfig(Config.taskConfigDef, config) {}
