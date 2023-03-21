package org.apache.celeborn.common.network

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util

import scala.collection.JavaConversions._

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging

/**
 * <p>
 * Simple {@link DNSToSwitchMapping} implementation that reads a 2 column text
 * file. The columns are separated by whitespace. The first column is a DNS or
 * IP address and the second column specifies the rack where the address maps.
 * </p>
 * <p>
 * This class uses the configuration parameter {@code
 * net.topology.table.file.name} to locate the mapping file.
 * </p>
 * <p>
 * Calls to {@link # resolve ( List )} will look up the address as defined in the
 * mapping file. If no entry corresponding to the address is found, the value
 * {@code /default-rack} is returned.
 * </p>
 */

class TableMapping(val rawMapping:DNSToSwitchMapping =  new TableMapping.RawTableMapping) extends CachedDNSToSwitchMapping(rawMapping) {
  private def getRawMapping: TableMapping.RawTableMapping = {
    rawMapping.asInstanceOf[TableMapping.RawTableMapping]
  }

  override def getConf: CelebornConf = {
    getRawMapping.getConf
  }

  override def setConf(conf: CelebornConf): Unit = {
    super.setConf(conf)
    getRawMapping.setConf(conf)
  }

  override def reloadCachedMappings(): Unit = {
    super.reloadCachedMappings()
    getRawMapping.reloadCachedMappings()
  }
}

object TableMapping {
  final private class RawTableMapping extends DNSToSwitchMapping with Logging {
    private var conf: CelebornConf = null
    private var map: java.util.Map[String, String] = null

    def setConf(conf: CelebornConf): Unit = {
      this.conf = conf
    }

    def getConf: CelebornConf = conf

    private def load: java.util.Map[String, String] = {
      val loadMap = new java.util.HashMap[String, String]
      val filename = getConf.get(CelebornConf.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY).orNull
      if (StringUtils.isBlank(filename)) {
        logWarning(CelebornConf.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY.key + " not configured. ")
        return null
      }
      try {
        val reader = new BufferedReader(new InputStreamReader(
          Files.newInputStream(Paths.get(filename)),
          StandardCharsets.UTF_8))
        try {
          var line = reader.readLine
          while (line != null) {
            line = line.trim
            if (line.length != 0 && line.charAt(0) != '#') {
              val columns = line.split("\\s+")
              if (columns.length == 2) {
                loadMap.put(columns(0), columns(1))
              } else {
                logWarning("Line does not have two columns. Ignoring. " + line)
              }
            }
            line = reader.readLine
          }
        } catch {
          case e: Exception =>
            logWarning(filename + " cannot be read.", e)
            return null
        } finally {
          if (reader != null) {
            reader.close()
          }
        }
      }
      loadMap
    }

    override def resolve(names: java.util.List[String]): java.util.List[String] = {
      if (map == null) {
        map = load
        if (map == null) {
          logWarning("Failed to read topology table. " + NetworkTopology.DEFAULT_RACK + " will be used for all nodes.")
          map = new java.util.HashMap[String, String]
        }
      }
      val results: java.util.List[String] = new java.util.ArrayList[String](names.size)

      for (name <- names) {
        val result: String = map.get(name)
        if (result != null) {
          results.add(result)
        } else {
          results.add(NetworkTopology.DEFAULT_RACK)
        }
      }
      results
    }

    override def reloadCachedMappings(): Unit = {
      val newMap: java.util.Map[String, String] = load
      if (newMap == null) {
        logError(
          "Failed to reload the topology table.  The cached " + "mappings will not be cleared.")
      } else {
        this synchronized {
          map = newMap
        }
      }
    }

    override def reloadCachedMappings(names: java.util.List[String]): Unit = {
      // TableMapping has to reload all mappings at once, so no chance to
      // reload mappings on specific nodes
      reloadCachedMappings()
    }
  }
}
