package org.apache.celeborn.common.network

import java.util

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.celeborn.common.CelebornConf

/**
 * This is a base class for DNS to Switch mappings. <p> It is not mandatory to
 * derive {@link DNSToSwitchMapping} implementations from it, but it is strongly
 * recommended, as it makes it easy for the Hadoop developers to add new methods
 * to this base class that are automatically picked up by all implementations.
 * <p>
 *
 * The constructor of the <code>Configured</code> calls
 * the  {@link # setConf ( Configuration )} method, which will call into the
 * subclasses before they have been fully constructed.
 */

abstract class AbstractDNSToSwitchMapping() extends DNSToSwitchMapping {

  /**
   * Create an unconfigured instance
   */
  private var conf: CelebornConf = null

  /**
   * Create an instance, caching the configuration file.
   * This constructor does not call {@link # setConf ( Configuration )}; if
   * a subclass extracts information in that method, it must call it explicitly.
   *
   * @param conf the configuration
   */
  def this(conf: CelebornConf) {
    this()
    this.conf = conf
  }

  def getConf: CelebornConf = conf

  def setConf(conf: CelebornConf): Unit = {
    this.conf = conf
  }

  /**
   * Predicate that indicates that the switch mapping is known to be
   * single-switch. The base class returns false: it assumes all mappings are
   * multi-rack. Subclasses may override this with methods that are more aware
   * of their topologies.
   *
   * <p>
   *
   * This method is used when parts of Hadoop need know whether to apply
   * single rack vs multi-rack policies, such as during block placement.
   * Such algorithms behave differently if they are on multi-switch systems.
   * </p>
   *
   * @return true if the mapping thinks that it is on a single switch
   */
  def isSingleSwitch: Boolean = false

  /**
   * Get a copy of the map (for diagnostics)
   *
   * @return a clone of the map or null for none known
   */
  def getSwitchMap: java.util.Map[String, String] = null

  /**
   * Generate a string listing the switch mapping implementation,
   * the mapping for every known node and the number of nodes and
   * unique switches known about -each entry to a separate line.
   *
   * @return a string that can be presented to the ops team or used in
   *         debug messages.
   */
  def dumpTopology: String = {
    val rack: java.util.Map[String, String] = getSwitchMap
    val builder: mutable.StringBuilder = new mutable.StringBuilder
    builder.append("Mapping: ").append(toString).append("\n")
    if (rack != null) {
      builder.append("Map:\n")
      val switches: java.util.Set[String] = new java.util.HashSet[String]

      for (entry <- rack.entrySet) {
        builder.append("  ").append(entry.getKey).append(" -> ").append(entry.getValue).append("\n")
        switches.add(entry.getValue)
      }
      builder.append("Nodes: ").append(rack.size).append("\n").append("Switches: ").append(
        switches.size).append("\n")
    } else builder.append("No topology information")
    builder.toString
  }

  protected def isSingleSwitchByScriptPolicy: Boolean = {
    conf != null && conf.get(CelebornConf.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY).isEmpty
  }
}

object AbstractDNSToSwitchMapping {

  /**
   * Query for a {@link DNSToSwitchMapping} instance being on a single
   * switch.
   * <p>
   * This predicate simply assumes that all mappings not derived from
   * this class are multi-switch.
   *
   * @param mapping the mapping to query
   * @return true if the base class says it is single switch, or the mapping
   *         is not derived from this class.
   */
  def isMappingSingleSwitch(mapping: DNSToSwitchMapping): Boolean = {
    mapping != null && mapping.isInstanceOf[AbstractDNSToSwitchMapping] && mapping.asInstanceOf[
      AbstractDNSToSwitchMapping].isSingleSwitch
  }
}
