package com.github.jmtoball.kafka.conf

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Range, Type}

object Config {
  val KEY_TOPIC: String = "topic"
  val KEY_INTERVAL: String = "interval"
  val KEY_PULL: String = "pull"
  val KEY_REWIND: String = "rewind"
  val KEY_PATHS: String = "paths"
  val KEY_PATH: String = "path"

  private def commonConfig() = {
    new ConfigDef()
      .define(KEY_TOPIC, Type.STRING, "git-log", ConfigDef.Importance.HIGH, "The topic to publish commits to")
      .define(KEY_INTERVAL, Type.INT, 10000, Range.atLeast(0), ConfigDef.Importance.HIGH, "The interval in which to poll each for new commits")
      .define(KEY_REWIND, Type.BOOLEAN, true, ConfigDef.Importance.LOW, "Whether or not to start publishing commits from the beginning of the history or only upcoming ones")
      .define(KEY_PULL, Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Whether or not to pull upstream commits into the working copy actively")
  }

  val connectorConfigDef: ConfigDef = commonConfig
    .define(KEY_PATHS, Type.LIST, ConfigDef.Importance.HIGH, "The repository paths to watch")
  val taskConfigDef: ConfigDef = commonConfig
    .define(KEY_PATH, Type.STRING, ConfigDef.Importance.HIGH, "The repository path to watch")
}
