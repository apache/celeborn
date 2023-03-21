package org.apache.celeborn.common.network

import java.util
import java.util.{ArrayList, HashMap, List, Map}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._

import org.apache.hadoop.net.{AbstractDNSToSwitchMapping, DNSToSwitchMapping, NetUtils}

import org.apache.celeborn.common.network.AbstractDNSToSwitchMapping.isMappingSingleSwitch

/**
 * A cached implementation of DNSToSwitchMapping that takes an
 * raw DNSToSwitchMapping and stores the resolved network location in
 * a cache. The following calls to a resolved network location
 * will get its location from the cache.
 */
class CachedDNSToSwitchMapping(rawMapping: DNSToSwitchMapping)
  extends AbstractDNSToSwitchMapping {
  private val cache: java.util.Map[String, String] = new ConcurrentHashMap[String, String]

  /**
   * @param names a list of hostnames to probe for being cached
   * @return the hosts from 'names' that have not been cached previously
   */
  private def getUncachedHosts(names: java.util.List[String]): java.util.List[String] = {
    // find out all names without cached resolved location
    val unCachedHosts: java.util.List[String] = new java.util.ArrayList[String](names.size)
    for (name <- names) {
      if (cache.get(name) == null) {
        unCachedHosts.add(name)
      }
    }
    unCachedHosts
  }

  /**
   * Caches the resolved host:rack mappings. The two list
   * parameters must be of equal size.
   *
   * @param uncachedHosts a list of hosts that were uncached
   * @param resolvedHosts a list of resolved host entries where the element
   *                      at index(i) is the resolved value for the entry in uncachedHosts[i]
   */
  private def cacheResolvedHosts(
      uncachedHosts: java.util.List[String],
      resolvedHosts: java.util.List[String]): Unit = { // Cache the result
    if (resolvedHosts != null) {
      for (i <- 0 until uncachedHosts.size) {
        cache.put(uncachedHosts.get(i), resolvedHosts.get(i))
      }
    }
  }

  /**
   * @param names a list of hostnames to look up (can be be empty)
   * @return the cached resolution of the list of hostnames/addresses.
   *         or null if any of the names are not currently in the cache
   */
  private def getCachedHosts(names: java.util.List[String]): java.util.List[String] = {
    val result: java.util.List[String] = new java.util.ArrayList[String](names.size)
    // Construct the result

    for (name <- names) {
      val networkLocation: String = cache.get(name)
      if (networkLocation != null) result.add(networkLocation)
      else return null
    }
    result
  }

  override def resolve(names: java.util.List[String]): java.util.List[String] = {
    // normalize all input names to be in the form of IP addresses
    val normalizedNames = NetUtils.normalizeHostNames(names)
    val result: java.util.List[String] = new java.util.ArrayList[String](normalizedNames.size)
    if (normalizedNames.isEmpty) {
      return result
    }
    val uncachedHosts: java.util.List[String] = getUncachedHosts(normalizedNames)
    // Resolve the uncached hosts
    val resolvedHosts: java.util.List[String] = rawMapping.resolve(uncachedHosts)
    // cache them
    cacheResolvedHosts(uncachedHosts, resolvedHosts)
    // now look up the entire list in the cache
    getCachedHosts(normalizedNames)
  }

  /**
   * Get the (host x switch) map.
   *
   * @return a copy of the cached map of hosts to rack
   */
  override def getSwitchMap: java.util.Map[String, String] = {
    new java.util.HashMap[String, String](cache)
  }

  override def toString: String = "cached switch mapping relaying to " + rawMapping

  /**
   * Delegate the switch topology query to the raw mapping, via
   * {@link AbstractDNSToSwitchMapping.isMappingSingleSwitch(DNSToSwitchMapping)}
   *
   * @return true iff the raw mapper is considered single-switch.
   */
  override def isSingleSwitch: Boolean = isMappingSingleSwitch(rawMapping)

  override def reloadCachedMappings(): Unit = {
    cache.clear()
  }

  override def reloadCachedMappings(names: java.util.List[String]): Unit = {
    for (name <- names) {
      cache.remove(name)
    }
  }
}
