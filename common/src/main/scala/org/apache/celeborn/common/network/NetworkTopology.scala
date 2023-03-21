package org.apache.celeborn.common.network

/**
 * The class represents a cluster of computer with a tree hierarchical
 * network topology.
 * For example, a cluster may be consists of many data centers filled
 * with racks of computers.
 * In a network topology, leaves represent data nodes (computers) and inner
 * nodes represent switches/routers that manage traffic in/out of data centers
 * or racks.
 */
object NetworkTopology {
  val DEFAULT_RACK = "/default-rack"
}
