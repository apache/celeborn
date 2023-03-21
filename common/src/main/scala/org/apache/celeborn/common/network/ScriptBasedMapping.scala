package org.apache.celeborn.common.network

import java.io.File
import java.util
import java.util.StringTokenizer

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.apache.hadoop.util.Shell

import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.internal.Logging

/**
 * This class implements the {@link DNSToSwitchMapping} interface using a
 * script configured via the
 * {@link CelebornConf.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY} option.
 * <p>
 * It contains a static class <code>RawScriptBasedMapping</code> that performs
 * the work: reading the configuration parameters, executing any defined
 * script, handling errors and such like. The outer
 * class extends {@link CachedDNSToSwitchMapping} to cache the delegated
 * queries.
 * <p>
 * This DNS mapper's {@link # isSingleSwitch ( )} predicate returns
 * true if and only if a script is defined.
 */
class ScriptBasedMapping(val rawMapping: DNSToSwitchMapping =
  new ScriptBasedMapping.RawScriptBasedMapping()) extends CachedDNSToSwitchMapping(rawMapping) {

  private def getRawMapping: ScriptBasedMapping.RawScriptBasedMapping =
    rawMapping.asInstanceOf[ScriptBasedMapping.RawScriptBasedMapping]

  override def getConf: CelebornConf = getRawMapping.getConf

  override def toString: String = "script-based mapping with " + getRawMapping.toString

  override def setConf(conf: CelebornConf): Unit = {
    super.setConf(conf)
    getRawMapping.setConf(conf)
  }
}

object ScriptBasedMapping {

  private val MIN_ALLOWABLE_ARGS: Int = 1
  val NO_SCRIPT: String = "no script"

  protected class RawScriptBasedMapping() extends AbstractDNSToSwitchMapping with Logging {

    private var scriptName: String = null
    private var maxArgs: Int = 0 // max hostnames per call of the script

    override def setConf(conf: CelebornConf): Unit = {
      super.setConf(conf)
      if (conf != null) {
        scriptName = conf.get(CelebornConf.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY).orNull
        maxArgs = conf.get(CelebornConf.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY)
      } else {
        scriptName = null
        maxArgs = 0
      }
    }

    override def resolve(names: java.util.List[String]): java.util.List[String] = {
      val m: java.util.List[String] = new java.util.ArrayList[String](names.size)
      if (names.isEmpty) return m
      if (scriptName == null) {

        for (name <- names) {
          m.add(NetworkTopology.DEFAULT_RACK)
        }
        return m
      }
      val output: String = runResolveCommand(names, scriptName)
      if (output != null) {
        val allSwitchInfo: StringTokenizer = new StringTokenizer(output)
        while ({
          allSwitchInfo.hasMoreTokens
        }) {
          val switchInfo: String = allSwitchInfo.nextToken
          m.add(switchInfo)
        }
        if (m.size != names.size) { // invalid number of entries returned by the script
          logError("Script " + scriptName + " returned " + Integer.toString(
            m.size) + " values when " + Integer.toString(names.size) + " were expected.")
          return null
        }
      } else { // an error occurred. return null to signify this.
        // (exn was already logged in runResolveCommand)
        return null
      }
      m
    }

    /**
     * Build and execute the resolution command. The command is
     * executed in the directory specified by the system property
     * "user.dir" if set; otherwise the current working directory is used
     *
     * @param args a list of arguments
     * @return null if the number of arguments is out of range,
     *         or the output of the command.
     */
    protected def runResolveCommand(args: java.util.List[String], commandScriptName: String): String = {
      var loopCount: Int = 0
      if (args.size == 0) return null
      val allOutput: mutable.StringBuilder = new mutable.StringBuilder
      var numProcessed: Int = 0
      if (maxArgs < MIN_ALLOWABLE_ARGS) {
        logWarning("Invalid value " + Integer.toString(
          maxArgs) + " for " + CelebornConf.NET_TOPOLOGY_SCRIPT_NUMBER_ARGS_KEY.key + "; must be >= " + Integer.toString(
          MIN_ALLOWABLE_ARGS))
        return null
      }
      while (numProcessed != args.size) {
        val start: Int = maxArgs * loopCount
        val cmdList: java.util.List[String] = new java.util.ArrayList[String]
        cmdList.add(commandScriptName)
        numProcessed = start
        while (numProcessed < (start + maxArgs) && numProcessed < args.size) {
          cmdList.add(args.get(numProcessed))
          numProcessed += 1
        }
        var dir: File = null
        val userDir: String = System.getProperty("user.dir")
        if (userDir != null) {
          dir = new File(userDir)
        }
        val s: Shell.ShellCommandExecutor =
          new Shell.ShellCommandExecutor(cmdList.toArray(new Array[String](cmdList.size)), dir)
        try {
          s.execute()
          allOutput.append(s.getOutput).append(" ")
        } catch {
          case e: Exception =>
            logWarning("Exception running " + s, e)
            return null
        }
        loopCount += 1
      }
      allOutput.toString
    }

    override def isSingleSwitch: Boolean = scriptName == null

    override def toString: String = {
      if (scriptName != null) {
        "script " + scriptName
      } else {
        NO_SCRIPT
      }
    }

    override def reloadCachedMappings(): Unit = {
      // Nothing to do here, since RawScriptBasedMapping has no cache, and
      // does not inherit from CachedDNSToSwitchMapping
    }

    override def reloadCachedMappings(names: java.util.List[String]): Unit = {}
  }
}
