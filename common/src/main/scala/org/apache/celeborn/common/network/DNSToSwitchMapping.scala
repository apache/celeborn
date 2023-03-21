package org.apache.celeborn.common.network

/**
 * An interface that must be implemented to allow pluggable
 * DNS-name/IP-address to RackID resolvers.
 */
trait DNSToSwitchMapping {

  /**
   * Resolves a list of DNS-names/IP-addresses and returns back a list of
   * switch information (network paths). One-to-one correspondence must be
   * maintained between the elements in the lists.
   * Consider an element in the argument list - x.y.com. The switch information
   * that is returned must be a network path of the form /foo/rack,
   * where / is the root, and 'foo' is the switch where 'rack' is connected.
   * Note the hostname/ip-address is not part of the returned path.
   * The network topology of the cluster would determine the number of
   * components in the network path.
   * <p>
   *
   * If a name cannot be resolved to a rack, the implementation
   * should return {@link NetworkTopology.DEFAULT_RACK}. This
   * is what the bundled implementations do, though it is not a formal requirement
   *
   * @param names the list of hosts to resolve (can be empty)
   * @return list of resolved network paths.
   *         If <i>names</i> is empty, the returned list is also empty
   */
  def resolve(names: java.util.List[String]): java.util.List[String]

  /**
   * Reload all of the cached mappings.
   *
   * If there is a cache, this method will clear it, so that future accesses
   * will get a chance to see the new data.
   */
  def reloadCachedMappings(): Unit

  /**
   * Reload cached mappings on specific nodes.
   *
   * If there is a cache on these nodes, this method will clear it, so that
   * future accesses will see updated data.
   */
  def reloadCachedMappings(names: java.util.List[String]): Unit
}
