package com.miraisolutions.spark.bigquery.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * File utilities.
  */
private[bigquery] object Files {

  /**
    * Returns a Hadoop filesystem and path for the provided path.
    * @param path File or directory path
    * @param conf Hadoop configuration
    * @return Hadoop filesystem and path
    */
  private def getFsAndPath(path: String, conf: Configuration): (FileSystem, Path) = {
    val p = new Path(path)
    val fs = FileSystem.get(p.toUri, conf)
    (fs, p)
  }

  /**
    * Deletes the specified path recursively.
    * @param path Path to delete
    * @param conf Hadoop configuration
    */
  def delete(path: String, conf: Configuration): Unit = {
    val (fs, p) = getFsAndPath(path, conf)
    fs.delete(p, true)
  }

  /**
    * Registers the specified path for deletion when the underlying filesystem is being closed.
    * @param path Path to delete
    * @param conf Hadoop configuration
    */
  def deleteOnExit(path: String, conf: Configuration): Unit = {
    val (fs, p) = getFsAndPath(path, conf)
    fs.deleteOnExit(p)
  }

}
