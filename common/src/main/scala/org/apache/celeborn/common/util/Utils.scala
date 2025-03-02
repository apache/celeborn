/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.celeborn.common.util

import java.io._
import java.lang.management.{LockInfo, ManagementFactory, MonitorInfo, ThreadInfo}
import java.lang.reflect.InvocationTargetException
import java.math.{MathContext, RoundingMode}
import java.net._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.util
import java.util.{Locale, Properties, Random, UUID}
import java.util.concurrent.{Callable, ThreadPoolExecutor, TimeoutException, TimeUnit}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.{Failure, Random => ScalaRandom, Success, Try}
import scala.util.control.{ControlThrowable, NonFatal}
import scala.util.matching.Regex

import com.google.protobuf.{ByteString, GeneratedMessageV3}
import io.netty.channel.unix.Errors.NativeIoException
import org.apache.commons.lang3.SystemUtils
import org.apache.commons.lang3.time.FastDateFormat
import org.roaringbitmap.RoaringBitmap

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.CelebornConf.PORT_MAX_RETRY
import org.apache.celeborn.common.exception.{CelebornException, CelebornIOException}
import org.apache.celeborn.common.internal.Logging
import org.apache.celeborn.common.meta.{DiskStatus, WorkerInfo}
import org.apache.celeborn.common.network.protocol.TransportMessage
import org.apache.celeborn.common.network.util.TransportConf
import org.apache.celeborn.common.protocol.{PartitionLocation, PartitionSplitMode, PartitionType, RpcNameConstants, TransportModuleConstants}
import org.apache.celeborn.common.protocol.message.{ControlMessages, Message, StatusCode}
import org.apache.celeborn.common.protocol.message.ControlMessages.WorkerResource
import org.apache.celeborn.reflect.DynConstructors

object Utils extends Logging {

  def stringToSeq(str: String): Seq[String] = {
    str.split(",").map(_.trim()).filter(_.nonEmpty)
  }

  def getSystemProperties: Map[String, String] = {
    System.getProperties.stringPropertyNames().asScala
      .map(key => (key, System.getProperty(key))).toMap
  }

  def timeStringAsMs(str: String): Long = {
    JavaUtils.timeStringAsMs(str)
  }

  def timeStringAsSeconds(str: String): Long = {
    JavaUtils.timeStringAsSec(str)
  }

  def byteStringAsBytes(str: String): Long = {
    JavaUtils.byteStringAsBytes(str)
  }

  def byteStringAsKb(str: String): Long = {
    JavaUtils.byteStringAsKb(str)
  }

  def byteStringAsMb(str: String): Long = {
    JavaUtils.byteStringAsMb(str)
  }

  def byteStringAsGb(str: String): Long = {
    JavaUtils.byteStringAsGb(str)
  }

  def memoryStringToMb(str: String): Int = {
    // Convert to bytes, rather than directly to MB, because when no units are specified the unit
    // is assumed to be bytes
    (JavaUtils.byteStringAsBytes(str) / 1024 / 1024).toInt
  }

  private[this] val siByteSizes =
    Array(1L << 60, 1L << 50, 1L << 40, 1L << 30, 1L << 20, 1L << 10, 1)
  private[this] val siByteSuffixes =
    Array("EiB", "PiB", "TiB", "GiB", "MiB", "KiB", "B")

  /**
   * Convert a quantity in bytes to a human-readable string such as "4.0 MiB".
   */
  def bytesToString(size: Long): String = {
    var i = 0
    while (i < siByteSizes.length - 1 && size < 2 * siByteSizes(i)) i += 1
    "%.1f %s".formatLocal(Locale.US, size.toDouble / siByteSizes(i), siByteSuffixes(i))
  }

  def bytesToString(size: BigInt): String = {
    val EiB = 1L << 60
    if (size.isValidLong) {
      // Common case, most sizes fit in 64 bits and all ops on BigInt are order(s) of magnitude
      // slower than Long/Double.
      bytesToString(size.toLong)
    } else if (size < BigInt(2L << 10) * EiB) {
      "%.1f EiB".formatLocal(Locale.US, BigDecimal(size) / EiB)
    } else {
      // The number is too large, show it in scientific notation.
      BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B"
    }
  }

  def megabytesToString(megabytes: Long): String = {
    bytesToString(megabytes * 1024L * 1024L)
  }

  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute
    val locale = Locale.US

    ms match {
      case t if t < second =>
        "%d ms".formatLocal(locale, t)
      case t if t < minute =>
        "%.1f s".formatLocal(locale, t.toFloat / second)
      case t if t < hour =>
        "%.1f m".formatLocal(locale, t.toFloat / minute)
      case t =>
        "%.2f h".formatLocal(locale, t.toFloat / hour)
    }
  }

  def nanoDurationToString(ns: Long): String = {
    val ms = 1000 * 1000L
    val second = 1000 * ms
    val minute = 60 * second
    val hour = 60 * minute
    val locale = Locale.US

    ns match {
      case t if t < ms =>
        "%d ns".formatLocal(locale, t)
      case t if t < second =>
        "%.1f ms".formatLocal(locale, t.toFloat / ms)
      case t if t < minute =>
        "%.1f s".formatLocal(locale, t.toFloat / second)
      case t if t < hour =>
        "%.1f m".formatLocal(locale, t.toFloat / minute)
      case t =>
        "%.2f h".formatLocal(locale, t.toFloat / hour)
    }
  }

  @throws(classOf[CelebornException])
  def extractHostPortFromCelebornUrl(celebornUrl: String): (String, Int) = {
    try {
      val uri = new java.net.URI(celebornUrl)
      val host = uri.getHost
      val port = uri.getPort
      if (uri.getScheme != "celeborn" ||
        host == null ||
        port < 0 ||
        (uri.getPath != null && uri.getPath.nonEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null ||
        uri.getUserInfo != null) {
        throw new CelebornException(s"Invalid master URL: $celebornUrl")
      }
      (host, port)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new CelebornException(s"Invalid master URL: $celebornUrl", e)
    }
  }

  @throws(classOf[CelebornException])
  def extractHostPortNameFromCelebornUrl(celebornUrl: String): (String, Int, String) = {
    try {
      val uri = new java.net.URI(celebornUrl)
      val host = uri.getHost
      val port = uri.getPort
      val name = uri.getUserInfo
      if (uri.getScheme != "celeborn" ||
        host == null ||
        port < 0 ||
        name == null ||
        (uri.getPath != null && uri.getPath.nonEmpty) || // uri.getPath returns "" instead of null
        uri.getFragment != null ||
        uri.getQuery != null) {
        throw new CelebornException(s"Invalid Celeborn URL: $celebornUrl")
      }
      (host, port, name)
    } catch {
      case e: java.net.URISyntaxException =>
        throw new CelebornException(s"Invalid Celeborn URL: $celebornUrl", e)
    }
  }

  def tryOrIOException[T](block: => T): T = {
    try {
      block
    } catch {
      case e: IOException =>
        logError("Exception encountered", e)
        throw e
      case NonFatal(e) =>
        logError("Exception encountered", e)
        throw new IOException(e)
    }
  }

  def tryLogNonFatalError(block: => Unit): Unit = {
    try {
      block
    } catch {
      case NonFatal(t) =>
        logError(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
    }
  }

  def tryOrExit(block: => Unit): Unit = {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable => throw t
    }
  }

  /**
   * Select a random integer within the specified range.
   *
   * @param from the lower bound of the range (inclusive)
   * @param until the upper bound of the range (exclusive)
   * @return a randomly selected integer within the range [from, until)
   */
  def selectRandomInt(from: Int, until: Int): Int = {
    ScalaRandom.nextInt(until - 1 - from) + from
  }

  def startServiceOnPort[T](
      startPort: Int,
      startService: Int => (T, Int),
      conf: CelebornConf,
      serviceName: String = ""): (T, Int) = {

    require(
      startPort == 0 || (1024 <= startPort && startPort < 65536),
      "startPort should be between 1024 and 65535 (inclusive), or 0 for a random free port.")

    val serviceString = if (serviceName.isEmpty) "" else s" '$serviceName'"
    val maxRetries = conf.portMaxRetries
    for (offset <- 0 to maxRetries) {
      // Do not increment port if startPort is 0, which is treated as a special port
      val tryPort =
        if (startPort == 0) {
          startPort
        } else {
          userPort(startPort, offset)
        }
      try {
        val (service, port) = startService(tryPort)
        logInfo(s"Successfully started service$serviceString on port $port.")
        return (service, port)
      } catch {
        case e: Exception if isBindCollision(e) =>
          if (offset >= maxRetries) {
            val exceptionMessage =
              if (startPort == 0) {
                s"${e.getMessage}: Service$serviceString failed after " +
                  s"$maxRetries retries (on a random free port)! " +
                  s"Consider explicitly setting the appropriate binding address for " +
                  s"the service$serviceString to the correct binding address."
              } else {
                s"${e.getMessage}: Service$serviceString failed after " +
                  s"$maxRetries retries (starting from $startPort)! Consider explicitly setting " +
                  s"the appropriate port for the service$serviceString to an available port " +
                  s"or increasing ${PORT_MAX_RETRY.key}."
              }
            val exception = new BindException(exceptionMessage)
            // restore original stack trace
            exception.setStackTrace(e.getStackTrace)
            throw exception
          }
          if (startPort == 0) {
            // As startPort 0 is for a random free port, it is most possibly binding address is
            // not correct.
            logWarning(s"Service$serviceString could not bind on a random free port. " +
              "You may check whether configuring an appropriate binding address.")
          } else {
            logWarning(s"Service$serviceString could not bind on port $tryPort. " +
              s"Attempting port ${tryPort + 1}.")
          }
      }
    }
    // Should never happen
    throw new CelebornException(s"Failed to start service$serviceString on port $startPort")
  }

  def userPort(base: Int, offset: Int): Int = {
    (base + offset - 1024) % (65536 - 1024) + 1024
  }

  @tailrec
  def isBindCollision(exception: Throwable): Boolean = {
    exception match {
      case e: BindException =>
        if (e.getMessage != null) {
          return true
        }
        isBindCollision(e.getCause)
      case e: NativeIoException =>
        e.getMessage != null && e.getMessage.startsWith("bind")
      case e: Exception => isBindCollision(e.getCause)
      case _ => false
    }
  }

  def encodeFileNameToURIRawPath(fileName: String): String = {
    require(!fileName.contains("/") && !fileName.contains("\\"))
    // `file` and `localhost` are not used. Just to prevent URI from parsing `fileName` as
    // scheme or host. The prefix "/" is required because URI doesn't accept a relative path.
    // We should remove it after we get the raw path.
    new URI("file", null, "localhost", -1, "/" + fileName, null, null).getRawPath.substring(1)
  }

  /**
   * Shuffle the elements of a collection into a random order, returning the
   * result in a new collection. Unlike scala.util.Random.shuffle, this method
   * uses a local random number generator, avoiding inter-thread contention.
   */
  def randomize[T: ClassTag](seq: TraversableOnce[T]): Seq[T] = {
    randomizeInPlace(seq.toArray)
  }

  /**
   * Shuffle the elements of an array into a random order, modifying the
   * original array. Returns the original array.
   */
  def randomizeInPlace[T](arr: Array[T], rand: Random = new Random): Array[T] = {
    for (i <- (arr.length - 1) to 1 by -1) {
      val j = rand.nextInt(i + 1)
      val tmp = arr(j)
      arr(j) = arr(i)
      arr(i) = tmp
    }
    arr
  }

  val isWindows: Boolean = SystemUtils.IS_OS_WINDOWS

  val isMac: Boolean = SystemUtils.IS_OS_MAC_OSX

  val isMacOnAppleSilicon: Boolean =
    SystemUtils.IS_OS_MAC_OSX && SystemUtils.OS_ARCH.equals("aarch64")

  private lazy val localIpAddress: InetAddress = findLocalInetAddress()

  private def findLocalInetAddress(): InetAddress = {
    val defaultIpOverride = System.getenv("CELEBORN_LOCAL_IP")
    if (defaultIpOverride != null) {
      InetAddress.getByName(defaultIpOverride)
    } else {
      val address = InetAddress.getLocalHost
      if (address.isLoopbackAddress) {
        // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
        // a better address using the local network interfaces
        // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
        // on unix-like system. On windows, it returns in index order.
        // It's more proper to pick ip address following system output order.
        val activeNetworkIFs = NetworkInterface.getNetworkInterfaces.asScala.toSeq
        val reOrderedNetworkIFs = if (isWindows) activeNetworkIFs else activeNetworkIFs.reverse

        for (ni <- reOrderedNetworkIFs) {
          val addresses = ni.getInetAddresses.asScala
            .filterNot(addr => addr.isLinkLocalAddress || addr.isLoopbackAddress).toSeq
          if (addresses.nonEmpty) {
            val addr = addresses.find(_.isInstanceOf[Inet4Address]).getOrElse(addresses.head)
            // because of Inet6Address.toHostName may add interface at the end if it knows about it
            val strippedAddress = InetAddress.getByAddress(addr.getAddress)
            // We've found an address that looks reasonable!
            logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
              " a loopback address: " + address.getHostAddress + "; using " +
              strippedAddress.getHostAddress + " instead (on interface " + ni.getName + ")")
            logWarning("Set CELEBORN_LOCAL_IP if you need to bind to another address")
            return strippedAddress
          }
        }
        logWarning("Your hostname, " + InetAddress.getLocalHost.getHostName + " resolves to" +
          " a loopback address: " + address.getHostAddress + ", but we couldn't find any" +
          " external IP address!")
        logWarning("Set CELEBORN_LOCAL_IP if you need to bind to another address")
      }
      address
    }
  }

  private var customHostname: Option[String] = sys.env.get("CELEBORN_LOCAL_HOSTNAME")

  // for testing
  def setCustomHostname(hostname: String) {
    Utils.checkHost(hostname)
    customHostname = Some(hostname)
  }

  def localHostName(conf: CelebornConf): String = {
    getHostName(conf.bindPreferIP)
  }

  def localHostNameForAdvertiseAddress(conf: CelebornConf, env: String): String = {
    if (env.equals(RpcNameConstants.MASTER_SYS) || env.equals(
        RpcNameConstants.MASTER_INTERNAL_SYS)) {
      getAdvertiseAddressForMaster(conf)
    } else {
      getAdvertiseAddressForWorker(conf)
    }
  }

  private def getAdvertiseAddressForMaster(conf: CelebornConf) = {
    conf.advertiseAddressMasterHost
  }

  private def getAdvertiseAddressForWorker(conf: CelebornConf) = {
    getHostName(conf.advertisePreferIP)
  }

  def getHostName(preferIP: Boolean): String = customHostname.getOrElse {
    if (preferIP) {
      localIpAddress match {
        case ipv6Address: Inet6Address =>
          val ip = ipv6Address.getHostAddress
          assert(
            !ip.startsWith("[") && !ip.endsWith("]"),
            s"Resolved IPv6 address should not be enclosed in [] but got $ip")
          s"[$ip]"
        case other => other.getHostAddress
      }
    } else {
      localIpAddress.getCanonicalHostName
    }
  }

  /**
   * Checks if the host contains only valid hostname/ip without port
   * NOTE: Incase of IPV6 ip it should be enclosed inside []
   */
  def checkHost(host: String): Unit = {
    if (host != null && host.split(":").length > 2) {
      assert(
        host.startsWith("[") && host.endsWith("]"),
        s"Expected hostname or IPv6 IP enclosed in [] but got $host")
    } else {
      assert(host != null && host.indexOf(':') == -1, s"Expected hostname or IP but got $host")
    }
  }

  private def getIpHostAddressPair(host: String): (String, String) = {
    try {
      val inetAddress = InetAddress.getByName(host)
      val hostAddress = inetAddress.getHostAddress
      if (host.equals(hostAddress)) {
        (hostAddress, inetAddress.getCanonicalHostName)
      } else {
        (hostAddress, host)
      }
    } catch {
      case _: Throwable => (host, host) // return original input
    }
  }

  // Convert address (ip:port or host:port) to (ip:port, host:port) pair
  def addressToIpHostAddressPair(address: String): (String, String) = {
    val (host, port) = Utils.parseHostPort(address)
    val (_ip, _host) = Utils.getIpHostAddressPair(host)
    (_ip + ":" + port, _host + ":" + port)
  }

  def checkHostPort(hostPort: String): Unit = {
    if (hostPort != null && hostPort.split(":").length > 2) {
      assert(
        hostPort != null && hostPort.indexOf("]:") != -1,
        s"Expected host and port but got $hostPort")
    } else {
      assert(
        hostPort != null && hostPort.indexOf(':') != -1,
        s"Expected host and port but got $hostPort")
    }
  }

  // Typically, this will be of order of number of nodes in cluster
  // If not, we should change it to LRUCache or something.
  private val hostPortParseResults = JavaUtils.newConcurrentHashMap[String, (String, Int)]()

  def parseHostPort(hostPort: String): (String, Int) = {
    // Check cache first.
    val cached = hostPortParseResults.get(hostPort)
    if (cached != null) {
      return cached
    }

    def setDefaultPortValue(): (String, Int) = {
      val retval = (hostPort, 0)
      hostPortParseResults.put(hostPort, retval)
      retval
    }
    // checks if the host:port contains IPV6 ip and parses the host, port
    if (hostPort != null && hostPort.split(":").length > 2) {
      val index: Int = hostPort.lastIndexOf("]:")
      if (-1 == index) {
        return setDefaultPortValue()
      }
      val port = hostPort.substring(index + 2).trim()
      val retVal = (hostPort.substring(0, index + 1).trim(), if (port.isEmpty) 0 else port.toInt)
      hostPortParseResults.putIfAbsent(hostPort, retVal)
    } else {
      val index: Int = hostPort.lastIndexOf(':')
      if (-1 == index) {
        return setDefaultPortValue()
      }
      val port = hostPort.substring(index + 1).trim()
      val retVal = (hostPort.substring(0, index).trim(), if (port.isEmpty) 0 else port.toInt)
      hostPortParseResults.putIfAbsent(hostPort, retVal)
    }

    hostPortParseResults.get(hostPort)
  }

  private var maxDefaultNettyThreads = 64

  def fromCelebornConf(
      _conf: CelebornConf,
      module: String,
      numUsableCores: Int = 0): TransportConf = {
    val conf = _conf.clone
    maxDefaultNettyThreads = conf.maxDefaultNettyThreads
    // Specify thread configuration based on our JVM's allocation of cores (rather than necessarily
    // assuming we have all the machine's cores).
    // NB: Only set if serverThreads/clientThreads not already set.
    val numThreads = defaultNumThreads(numUsableCores)
    conf.setTransportConfIfMissing(
      module,
      CelebornConf.NETWORK_IO_SERVER_THREADS,
      numThreads.toString)
    conf.setTransportConfIfMissing(
      module,
      CelebornConf.NETWORK_IO_CLIENT_THREADS,
      numThreads.toString)
    if (TransportModuleConstants.PUSH_MODULE == module) {
      conf.setTransportConfIfMissing(
        module,
        CelebornConf.NETWORK_IO_NUM_CONNECTIONS_PER_PEER,
        numThreads.toString)
    }

    new TransportConf(module, conf)
  }

  private def defaultNumThreads(numUsableCores: Int): Int = {
    val availableCores =
      if (numUsableCores > 0) numUsableCores else Runtime.getRuntime.availableProcessors()
    math.min(availableCores, maxDefaultNettyThreads)
  }

  def getClassLoader: ClassLoader = getClass.getClassLoader

  def getContextOrClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClassLoader)

  def classIsLoadable(clazz: String): Boolean = {
    Try {
      // scalastyle:off classforname
      Class.forName(clazz, false, getContextOrClassLoader)
      // scalastyle:on classforname
    }.isSuccess
  }

  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    // scalastyle:off classforname
    Class.forName(className, true, getContextOrClassLoader)
    // scalastyle:on classforname
  }

  def instantiateMasterEndpointResolver[T](
      className: String,
      conf: CelebornConf,
      isWorker: Boolean): T = {
    try {
      DynConstructors.builder().impl(className, classOf[CelebornConf], java.lang.Boolean.TYPE)
        .build[T]()
        .newInstance(conf, java.lang.Boolean.valueOf(isWorker))
    } catch {
      case e: Throwable =>
        throw new CelebornException(s"Failed to instantiate masterEndpointResolver $className.", e)
    }
  }

  def instantiateDynamicConfigStoreBackend[T](className: String, conf: CelebornConf): T = {
    try {
      DynConstructors.builder().impl(className, classOf[CelebornConf])
        .build[T]()
        .newInstance(conf)
    } catch {
      case e: Throwable =>
        throw new CelebornException(
          s"Failed to instantiate dynamic config store backend $className.",
          e)
    }
  }

  def getCodeSourceLocation(clazz: Class[_]): String = {
    new File(clazz.getProtectionDomain.getCodeSource.getLocation.toURI).getPath
  }

  def loadDefaultCelebornProperties(conf: CelebornConf, filePath: String = null): String = {
    val path = Option(filePath).getOrElse(getDefaultPropertiesFile())
    Option(path).foreach { confFile =>
      getPropertiesFromFile(confFile).filter { case (k, v) =>
        k.startsWith("celeborn.")
      }.foreach { case (k, v) =>
        conf.setIfMissing(k, v)
        sys.props.getOrElseUpdate(k, v)
      }
    }
    path
  }

  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("CELEBORN_CONF_DIR")
      .orElse(env.get("CELEBORN_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}celeborn-defaults.conf") }
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }

  private[util] def trimExceptCRLF(str: String): String = {
    val nonSpaceOrNaturalLineDelimiter: Char => Boolean = { ch =>
      ch > ' ' || ch == '\r' || ch == '\n'
    }

    val firstPos = str.indexWhere(nonSpaceOrNaturalLineDelimiter)
    val lastPos = str.lastIndexWhere(nonSpaceOrNaturalLineDelimiter)
    if (firstPos >= 0 && lastPos >= 0) {
      str.substring(firstPos, lastPos + 1)
    } else {
      ""
    }
  }

  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala
        .map { k => (k, trimExceptCRLF(properties.getProperty(k))) }
        .toMap

    } catch {
      case e: IOException =>
        throw new CelebornException(s"Failed when loading Celeborn properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  def makeShuffleKey(applicationId: String, shuffleId: Int): String = {
    s"$applicationId-$shuffleId"
  }

  def splitShuffleKey(shuffleKey: String): (String, Int) = {
    val splits = shuffleKey.split("-")
    val appId = splits.dropRight(1).mkString("-")
    val shuffleId = splits.last.toInt
    (appId, shuffleId)
  }

  def splitPartitionLocationUniqueId(uniqueId: String): (Int, Int) = {
    val splits = uniqueId.split("-")
    val partitionId = splits.dropRight(1).mkString("-").toInt
    val epoch = splits.last.toInt
    (partitionId, epoch)
  }

  def makeReducerKey(shuffleId: Int, partitionId: Int): String = {
    s"$shuffleId-$partitionId"
  }

  def makeMapKey(applicationId: String, shuffleId: Int, mapId: Int, attemptId: Int): String = {
    s"$applicationId-$shuffleId-$mapId-$attemptId"
  }

  def makeMapKey(shuffleId: Int, mapId: Int, attemptId: Int): String = {
    s"$shuffleId-$mapId-$attemptId"
  }

  def shuffleKeyPrefix(shuffleKey: String): String = {
    shuffleKey + "-"
  }

  def bytesToInt(bytes: Array[Byte], bigEndian: Boolean = true): Int = {
    if (bigEndian) {
      bytes(0) << 24 | bytes(1) << 16 | bytes(2) << 8 | bytes(3)
    } else {
      bytes(3) << 24 | bytes(2) << 16 | bytes(1) << 8 | bytes(0)
    }
  }

  def timeIt(f: => Unit): Long = {
    val start = System.currentTimeMillis
    f
    System.currentTimeMillis - start
  }

  /**
   * Note: code was initially copied from Apache Spark(v3.5.1).
   */
  implicit private class Lock(lock: LockInfo) {
    def lockString: String = {
      lock match {
        case monitor: MonitorInfo => s"Monitor(${monitor.toString})"
        case _ => s"Lock(${lock.toString})"
      }
    }
  }

  /**
   * Return a thread dump of all threads' stacktraces.
   *
   * <p>Note: code was initially copied from Apache Spark(v3.5.1).
   */
  def getThreadDump(): Seq[ThreadStackTrace] = {
    // We need to filter out null values here because dumpAllThreads() may return null array
    // elements for threads that are dead / don't exist.
    ManagementFactory.getThreadMXBean.dumpAllThreads(true, true).filter(_ != null)
      .sortWith { case (threadTrace1, threadTrace2) =>
        val name1 = threadTrace1.getThreadName().toLowerCase(Locale.ROOT)
        val name2 = threadTrace2.getThreadName().toLowerCase(Locale.ROOT)
        val nameCmpRes = name1.compareTo(name2)
        if (nameCmpRes == 0) {
          threadTrace1.getThreadId < threadTrace2.getThreadId
        } else {
          nameCmpRes < 0
        }
      }.map(threadInfoToThreadStackTrace)
  }

  /**
   * Note: code was initially copied from Apache Spark(v3.5.1).
   */
  private def threadInfoToThreadStackTrace(threadInfo: ThreadInfo): ThreadStackTrace = {
    val threadState = threadInfo.getThreadState
    val monitors = threadInfo.getLockedMonitors.map(m => m.getLockedStackDepth -> m.toString).toMap
    val stackTrace = StackTrace(threadInfo.getStackTrace.zipWithIndex.map { case (frame, idx) =>
      val locked =
        if (idx == 0 && threadInfo.getLockInfo != null) {
          threadState match {
            case Thread.State.BLOCKED =>
              s"\t-  blocked on ${threadInfo.getLockInfo}\n"
            case Thread.State.WAITING | Thread.State.TIMED_WAITING =>
              s"\t-  waiting on ${threadInfo.getLockInfo}\n"
            case _ => ""
          }
        } else ""
      val locking = monitors.get(idx).map(mi => s"\t-  locked $mi\n").getOrElse("")
      s"${frame.toString}\n$locked$locking"
    })

    val synchronizers = threadInfo.getLockedSynchronizers.map(_.toString)
    val monitorStrs = monitors.values.toSeq
    ThreadStackTrace(
      threadInfo.getThreadId,
      threadInfo.getThreadName,
      threadState,
      stackTrace,
      if (threadInfo.getLockOwnerId < 0) None else Some(threadInfo.getLockOwnerId),
      Option(threadInfo.getLockInfo).map(_.lockString).getOrElse(""),
      synchronizers ++ monitorStrs,
      synchronizers,
      monitorStrs,
      Option(threadInfo.getLockName),
      Option(threadInfo.getLockOwnerName),
      threadInfo.isSuspended,
      threadInfo.isInNative)
  }

  private def readProcessStdout(process: Process): String = {
    val source = Source.fromInputStream(process.getInputStream)
    try {
      source.mkString
    } finally {
      source.close()
    }
  }

  def runCommand(cmd: String): String = {
    val process = Runtime.getRuntime.exec(cmd)
    readProcessStdout(process)
  }

  def runCommandComplex(cmd: String): String = {
    val cmds = Array("/bin/sh", "-c", cmd)
    val process = Runtime.getRuntime.exec(cmds)
    readProcessStdout(process)
  }

  /**
   * Execute a command and return the process running the command.
   */
  def executeCommand(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): Process = {
    val builder = new ProcessBuilder(command: _*).directory(workingDir)
    val environment = builder.environment()
    for ((key, value) <- extraEnvironment) {
      environment.put(key, value)
    }
    val process = builder.start()
    if (redirectStderr) {
      val threadName = "redirect stderr for command " + command.head

      def log(s: String): Unit = logInfo(s)

      processStreamByLine(threadName, process.getErrorStream, log)
    }
    process
  }

  /**
   * Execute a command and get its output, throwing an exception if it yields a code other than 0.
   */
  def executeAndGetOutput(
      command: Seq[String],
      workingDir: File = new File("."),
      extraEnvironment: Map[String, String] = Map.empty,
      redirectStderr: Boolean = true): String = {
    val process = executeCommand(command, workingDir, extraEnvironment, redirectStderr)
    val output = new StringBuilder
    val threadName = "read stdout for " + command.head

    def appendToOutput(s: String): Unit = output.append(s).append("\n")

    val stdoutThread = processStreamByLine(threadName, process.getInputStream, appendToOutput)
    val exitCode = process.waitFor()
    stdoutThread.join() // Wait for it to finish reading output
    if (exitCode != 0) {
      logError(s"Process $command exited with code $exitCode: $output")
      throw new CelebornException(s"Process $command exited with code $exitCode")
    }
    output.toString
  }

  /**
   * Return and start a daemon thread that processes the content of the input stream line by line.
   */
  def processStreamByLine(
      threadName: String,
      inputStream: InputStream,
      processLine: String => Unit): Thread = {
    val t = ThreadUtils.newDaemonThread(
      new Runnable {
        override def run(): Unit = {
          for (line <- Source.fromInputStream(inputStream).getLines()) {
            processLine(line)
          }
        }
      },
      threadName)
    t.start()
    t
  }

  /**
   * Create a directory inside the given parent directory. The directory is guaranteed to be
   * newly created, and is not marked for automatic deletion.
   */
  def createDirectory(root: String, namePrefix: String = "celeborn"): File = {
    var attempts = 0
    val maxAttempts = 10
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case _: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }

  /**
   * Create a temporary directory inside the given parent directory. The directory will be
   * automatically deleted when the VM shuts down.
   */
  def createTempDir(
      root: String = System.getProperty("java.io.tmpdir"),
      namePrefix: String = "celeborn"): File = {
    val dir = createDirectory(root, namePrefix)
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        if (dir != null) {
          JavaUtils.deleteRecursively(dir)
        }
      }
    }))
    dir
  }

  def mkString(args: Seq[String], sep: String = ","): String = {
    args.mkString(sep)
  }

  /**
   * @param slots
   * @return return a worker related slots usage by disk
   */
  def getSlotsPerDisk(
      slots: WorkerResource): util.Map[WorkerInfo, util.Map[String, Integer]] = {
    val workerSlotsDistribution = new util.HashMap[WorkerInfo, util.Map[String, Integer]]()
    slots.asScala.foreach { case (workerInfo, (primaryPartitionLoc, replicaPartitionLoc)) =>
      val diskSlotsMap = new util.HashMap[String, Integer]()

      def countSlotsByDisk(location: util.List[PartitionLocation]): Unit = {
        location.asScala.foreach(item => {
          val mountPoint = item.getStorageInfo.getMountPoint
          if (diskSlotsMap.containsKey(mountPoint)) {
            diskSlotsMap.put(mountPoint, 1 + diskSlotsMap.get(mountPoint))
          } else {
            diskSlotsMap.put(mountPoint, 1)
          }
        })
      }

      countSlotsByDisk(primaryPartitionLoc)
      countSlotsByDisk(replicaPartitionLoc)
      workerSlotsDistribution.put(workerInfo, diskSlotsMap)
    }
    workerSlotsDistribution
  }

  def getSlotsPerDisk(
      masterLocations: util.List[PartitionLocation],
      workerLocations: util.List[PartitionLocation]): util.Map[String, Integer] = {
    val slotDistributions = new util.HashMap[String, Integer]()
    (masterLocations.asScala ++ workerLocations.asScala)
      .foreach {
        location =>
          val mountPoint = location.getStorageInfo.getMountPoint
          if (slotDistributions.containsKey(mountPoint)) {
            slotDistributions.put(mountPoint, slotDistributions.get(mountPoint) + 1)
          } else {
            slotDistributions.put(mountPoint, 1)
          }
      }
    logDebug(s"locations to distribution, " +
      s"${masterLocations.asScala.map(_.toString).mkString(",")} " +
      s"${workerLocations.asScala.map(_.toString).mkString(",")} " +
      s"to $slotDistributions ")
    slotDistributions
  }

  /**
   * if the action is timeout, will return the callback result
   * if other exception will be thrown directly
   * @param block the normal action block
   * @param callback callback if timeout
   * @param timeoutInSeconds timeout limit value in seconds
   * @tparam T result type
   * @return result
   */
  def tryWithTimeoutAndCallback[T](block: => T)(callback: => T)(
      threadPool: ThreadPoolExecutor,
      timeoutInSeconds: Long = 10,
      errorMessage: String = "none"): T = {
    val futureTask = new Callable[T] {
      override def call(): T = {
        block
      }
    }

    var future: java.util.concurrent.Future[T] = null
    try {
      future = threadPool.submit(futureTask)
      future.get(timeoutInSeconds, TimeUnit.SECONDS)
    } catch {
      case _: TimeoutException =>
        logError(s"TimeoutException in thread ${Thread.currentThread().getName}," +
          s" error message: $errorMessage")
        callback
      case throwable: Throwable =>
        throw throwable
    } finally {
      if (null != future && !future.isCancelled) {
        future.cancel(true)
      }
    }
  }

  def tryWithResources[R <: Closeable, U](f: => R)(func: R => U): U = {
    val res = f
    try {
      func(f)
    } finally {
      if (null != res) {
        res.close()
      }
    }
  }

  def toTransportMessage(message: Any): Any = {
    message match {
      case legacy: Message =>
        ControlMessages.toTransportMessage(legacy)
      case pb: GeneratedMessageV3 =>
        ControlMessages.toTransportMessage(pb)
      case _ =>
        message
    }
  }

  def fromTransportMessage(message: Any): Any = {
    message match {
      case transportMessage: TransportMessage =>
        ControlMessages.fromTransportMessage(transportMessage)
      case _ => message
    }
  }

  def toStatusCode(status: Int): StatusCode = {
    status match {
      case 0 =>
        StatusCode.SUCCESS
      case 1 =>
        StatusCode.PARTIAL_SUCCESS
      case 2 =>
        StatusCode.REQUEST_FAILED
      case 3 =>
        StatusCode.SHUFFLE_ALREADY_REGISTERED
      case 4 =>
        StatusCode.SHUFFLE_NOT_REGISTERED
      case 5 =>
        StatusCode.RESERVE_SLOTS_FAILED
      case 6 =>
        StatusCode.SLOT_NOT_AVAILABLE
      case 7 =>
        StatusCode.WORKER_NOT_FOUND
      case 8 =>
        StatusCode.PARTITION_NOT_FOUND
      case 9 =>
        StatusCode.REPLICA_PARTITION_NOT_FOUND
      case 10 =>
        StatusCode.DELETE_FILES_FAILED
      case 11 =>
        StatusCode.PARTITION_EXISTS
      case 12 =>
        StatusCode.REVIVE_FAILED
      case 13 =>
        StatusCode.REPLICATE_DATA_FAILED
      case 14 =>
        StatusCode.NUM_MAPPER_ZERO
      case 15 =>
        StatusCode.MAP_ENDED
      case 16 =>
        StatusCode.STAGE_ENDED
      case 17 =>
        StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_PRIMARY
      case 18 =>
        StatusCode.PUSH_DATA_WRITE_FAIL_REPLICA
      case 19 =>
        StatusCode.PUSH_DATA_WRITE_FAIL_PRIMARY
      case 20 =>
        StatusCode.PUSH_DATA_FAIL_PARTITION_NOT_FOUND
      case 21 =>
        StatusCode.HARD_SPLIT
      case 22 =>
        StatusCode.SOFT_SPLIT
      case 23 =>
        StatusCode.STAGE_END_TIME_OUT
      case 24 =>
        StatusCode.SHUFFLE_DATA_LOST
      case 25 =>
        StatusCode.WORKER_SHUTDOWN
      case 26 =>
        StatusCode.NO_AVAILABLE_WORKING_DIR
      case 27 =>
        StatusCode.WORKER_EXCLUDED
      case 28 =>
        StatusCode.WORKER_UNKNOWN
      case 29 =>
        StatusCode.COMMIT_FILE_EXCEPTION
      case 30 =>
        StatusCode.PUSH_DATA_SUCCESS_PRIMARY_CONGESTED
      case 31 =>
        StatusCode.PUSH_DATA_SUCCESS_REPLICA_CONGESTED
      case 32 =>
        StatusCode.PUSH_DATA_HANDSHAKE_FAIL_REPLICA
      case 33 =>
        StatusCode.PUSH_DATA_HANDSHAKE_FAIL_PRIMARY
      case 34 =>
        StatusCode.REGION_START_FAIL_REPLICA
      case 35 =>
        StatusCode.REGION_START_FAIL_PRIMARY
      case 36 =>
        StatusCode.REGION_FINISH_FAIL_REPLICA
      case 37 =>
        StatusCode.REGION_FINISH_FAIL_PRIMARY
      case 38 =>
        StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_PRIMARY
      case 39 =>
        StatusCode.PUSH_DATA_CREATE_CONNECTION_FAIL_REPLICA
      case 40 =>
        StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_PRIMARY
      case 41 =>
        StatusCode.PUSH_DATA_CONNECTION_EXCEPTION_REPLICA
      case 42 =>
        StatusCode.PUSH_DATA_TIMEOUT_PRIMARY
      case 43 =>
        StatusCode.PUSH_DATA_TIMEOUT_REPLICA
      case 44 =>
        StatusCode.PUSH_DATA_PRIMARY_WORKER_EXCLUDED
      case 45 =>
        StatusCode.PUSH_DATA_REPLICA_WORKER_EXCLUDED
      case 46 =>
        StatusCode.FETCH_DATA_TIMEOUT
      case 47 =>
        StatusCode.REVIVE_INITIALIZED
      case 48 =>
        StatusCode.DESTROY_SLOTS_MOCK_FAILURE
      case 49 =>
        StatusCode.COMMIT_FILES_MOCK_FAILURE
      case 50 =>
        StatusCode.PUSH_DATA_FAIL_NON_CRITICAL_CAUSE_REPLICA
      case 51 =>
        StatusCode.OPEN_STREAM_FAILED
      case 52 =>
        StatusCode.SEGMENT_START_FAIL_REPLICA
      case 53 =>
        StatusCode.SEGMENT_START_FAIL_PRIMARY
      case _ =>
        null
    }
  }

  def toShuffleSplitMode(mode: Int): PartitionSplitMode = {
    mode match {
      case 0 => PartitionSplitMode.SOFT
      case 1 => PartitionSplitMode.HARD
      case _ =>
        logWarning(s"invalid shuffle mode $mode, fallback to soft")
        PartitionSplitMode.SOFT
    }
  }

  def toPartitionType(value: Int): PartitionType = {
    value match {
      case 0 => PartitionType.REDUCE
      case 1 => PartitionType.MAP
      case 2 => PartitionType.MAPGROUP
      case _ =>
        logWarning(s"invalid partitionType $value, fallback to ReducePartition")
        PartitionType.REDUCE
    }
  }

  def toDiskStatus(value: Int): DiskStatus = {
    value match {
      case 0 => DiskStatus.HEALTHY
      case 1 => DiskStatus.READ_OR_WRITE_FAILURE
      case 2 => DiskStatus.IO_HANG
      case 3 => DiskStatus.HIGH_DISK_USAGE
      case 4 => DiskStatus.CRITICAL_ERROR
      case _ => null
    }
  }

  def getPeerPath(path: String): String = {
    if (path.endsWith("0")) {
      path.substring(0, path.length - 1) + "1"
    } else {
      path.substring(0, path.length - 1) + "0"
    }
  }

  val SORTED_SUFFIX = ".sorted"
  val INDEX_SUFFIX = ".index"
  val SUFFIX_HDFS_WRITE_SUCCESS = ".success"
  val COMPATIBLE_HDFS_REGEX = "^(?!s3a://)[a-zA-Z0-9]+://.*"
  val S3_REGEX = "^s3[a]?://([a-z0-9][a-z0-9-]{1,61}[a-z0-9])(/.*)?$"

  val UNKNOWN_APP_SHUFFLE_ID = -1

  def isHdfsPath(path: String): Boolean = {
    path.matches(COMPATIBLE_HDFS_REGEX)
  }

  def isS3Path(path: String): Boolean = {
    path.matches(S3_REGEX)
  }

  def getSortedFilePath(path: String): String = {
    path + SORTED_SUFFIX
  }

  def getIndexFilePath(path: String): String = {
    path + INDEX_SUFFIX
  }

  def getWriteSuccessFilePath(path: String): String = {
    path + SUFFIX_HDFS_WRITE_SUCCESS
  }

  def roaringBitmapToByteString(roaringBitMap: RoaringBitmap): ByteString = {
    if (roaringBitMap != null && !roaringBitMap.isEmpty) {
      val buf = ByteBuffer.allocate(roaringBitMap.serializedSizeInBytes())
      roaringBitMap.serialize(buf)
      buf.rewind()
      ByteString.copyFrom(buf)
    } else {
      ByteString.EMPTY
    }
  }

  def byteStringToRoaringBitmap(bytes: ByteString): RoaringBitmap = {
    if (!bytes.isEmpty) {
      val roaringBitmap = new RoaringBitmap()
      val buf = bytes.asReadOnlyByteBuffer()
      buf.rewind()
      roaringBitmap.deserialize(buf)
      roaringBitmap
    } else {
      null
    }
  }

  def checkedDownCast(value: Long): Int = {
    val downCast = value.toInt
    if (downCast.toLong != value) {
      throw new IllegalArgumentException("Cannot downcast long value " + value + " to integer.")
    }
    downCast
  }

  @throws[IOException]
  def checkFileIntegrity(fileChannel: FileChannel, length: Int): Unit = {
    val remainingBytes = fileChannel.size - fileChannel.position
    if (remainingBytes < length) {
      logError(
        s"File remaining bytes not not enough, remaining: ${remainingBytes}, wanted: ${length}.")
      throw new RuntimeException(s"File is corrupted ${fileChannel}")
    }
  }

  def parseMetricLabels(label: String): (String, String) = {
    val labelPart = label.split("=")
    if (labelPart.size != 2) {
      throw new IllegalArgumentException(s"Illegal metric extra labels: $label")
    }
    labelPart(0).trim -> labelPart(1).trim
  }

  def getProcessId: String = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)

  private val dateFmt: FastDateFormat =
    FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ROOT)
  def formatTimestamp(timestamp: Long): String = dateFmt.format(timestamp)

  def parseColonSeparatedHostPorts(id: String, portsNum: Int): Array[String] = {
    val components = id.split(":")
    val portsArr = components.takeRight(portsNum)
    val host = components.dropRight(portsNum).mkString(":")
    Array(host) ++ portsArr
  }

  private val REDACTION_REPLACEMENT_TEXT = "*********(redacted)"

  /**
   * Redact the sensitive values in the given map. If a map key matches the redaction pattern then
   * its value is replaced with a dummy text.
   */
  def redact(conf: CelebornConf, kvs: Seq[(String, String)]): Seq[(String, String)] = {
    val redactionPattern = conf.secretRedactionPattern
    redact(redactionPattern, kvs)
  }

  private def redact[K, V](redactionPattern: Regex, kvs: Seq[(K, V)]): Seq[(K, V)] = {
    // If the sensitive information regex matches with either the key or the value, redact the value
    // While the original intent was to only redact the value if the key matched with the regex,
    // we've found that especially in verbose mode, the value of the property may contain sensitive
    // information like so:
    //
    // celeborn.dynamicConfig.store.db.hikari.password=secret_password ...
    //
    // And, in such cases, simply searching for the sensitive information regex in the key name is
    // not sufficient. The values themselves have to be searched as well and redacted if matched.
    // This does mean we may be accounting more false positives - for example, if the value of an
    // arbitrary property contained the term 'password', we may redact the value from the UI and
    // logs. In order to work around it, user would have to make the celeborn.redaction.regex property
    // more specific.
    kvs.map {
      case (key: String, value: String) =>
        redactionPattern.findFirstIn(key)
          .orElse(redactionPattern.findFirstIn(value))
          .map { _ => (key, REDACTION_REPLACEMENT_TEXT) }
          .getOrElse((key, value))
      case (key, value: String) =>
        redactionPattern.findFirstIn(value)
          .map { _ => (key, REDACTION_REPLACEMENT_TEXT) }
          .getOrElse((key, value))
      case (key, value) =>
        (key, value)
    }.asInstanceOf[Seq[(K, V)]]
  }

  def instantiate[T](className: String)(implicit tag: scala.reflect.ClassTag[T]): T = {
    logDebug(s"Creating instance of $className")
    val clazz = Class.forName(
      className,
      true,
      Thread.currentThread().getContextClassLoader).asInstanceOf[Class[T]]
    try {
      val ctor = clazz.getDeclaredConstructor()
      logDebug("Using no-arg constructor")
      ctor.newInstance()
    } catch {
      case e: NoSuchMethodException =>
        logError(s"Failed to instantiate class $className", e)
        throw e
    }
  }

  def isCriticalCauseForFetch(e: Exception) = {
    val rpcTimeout =
      e.isInstanceOf[IOException] && e.getCause != null && e.getCause.isInstanceOf[TimeoutException]
    val connectException =
      e.isInstanceOf[CelebornIOException] && e.getMessage != null && (e.getMessage.startsWith(
        "Connecting to") || e.getMessage.startsWith("Failed to"))
    val fetchChunkTimeout = e.isInstanceOf[
      CelebornIOException] && e.getCause != null && e.getCause.isInstanceOf[IOException]
    connectException || rpcTimeout || fetchChunkTimeout
  }

  /**
   * Create instances of extension classes.
   *
   * The classes in the given list must:
   * - Be subclasses of the given base class.
   * - Provide either a no-arg constructor, or a 1-arg constructor that takes a CelebornConf.
   *
   * The constructors are allowed to throw "UnsupportedOperationException" if the extension does not
   * want to be registered; this allows the implementations to check the Celeborn configuration (or
   * other state) and decide they do not need to be added. A log message is printed in that case.
   * Other exceptions are bubbled up.
   */
  def loadExtensions[T <: AnyRef](
      extClass: Class[T],
      classes: Seq[String],
      conf: CelebornConf): Seq[T] = {
    classes.flatMap { name =>
      try {
        val klass = classForName(name)
        require(
          extClass.isAssignableFrom(klass),
          s"$name is not a subclass of ${extClass.getName}.")

        val ext = Try(klass.getConstructor(classOf[CelebornConf])) match {
          case Success(ctor) =>
            ctor.newInstance(conf).asInstanceOf[T]

          case Failure(_) =>
            klass.getConstructor().newInstance().asInstanceOf[T]
        }

        Some(ext)
      } catch {
        case _: NoSuchMethodException =>
          throw new CelebornException(
            s"$name did not have a zero-argument constructor or a" +
              " single-argument constructor that accepts SparkConf. Note: if the class is" +
              " defined inside of another Scala class, then its constructors may accept an" +
              " implicit parameter that references the enclosing class; in this case, you must" +
              " define the class as a top-level class in order to prevent this extra" +
              " parameter from breaking Spark's ability to find a valid constructor.")

        case e: InvocationTargetException =>
          e.getCause match {
            case uoe: UnsupportedOperationException =>
              logDebug(s"Extension $name not being initialized.", uoe)
              logInfo("Extension ${MDC(CLASS_NAME, name)} not being initialized.")
              None

            case null => throw e

            case cause => throw cause
          }
      }
    }
  }

}
