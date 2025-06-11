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


package org.apache.celeborn.rest.v1.model;

import java.util.Objects;
import java.util.Arrays;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * ThreadStack
 */
@JsonPropertyOrder({
  ThreadStack.JSON_PROPERTY_THREAD_ID,
  ThreadStack.JSON_PROPERTY_THREAD_NAME,
  ThreadStack.JSON_PROPERTY_THREAD_STATE,
  ThreadStack.JSON_PROPERTY_STACK_TRACE,
  ThreadStack.JSON_PROPERTY_BLOCKED_BY_THREAD_ID,
  ThreadStack.JSON_PROPERTY_BLOCKED_BY_LOCK,
  ThreadStack.JSON_PROPERTY_HOLDING_LOCKS,
  ThreadStack.JSON_PROPERTY_SYNCHRONIZERS,
  ThreadStack.JSON_PROPERTY_MONITORS,
  ThreadStack.JSON_PROPERTY_LOCK_NAME,
  ThreadStack.JSON_PROPERTY_LOCK_OWNER_NAME,
  ThreadStack.JSON_PROPERTY_SUSPENDED,
  ThreadStack.JSON_PROPERTY_IN_NATIVE
})
@javax.annotation.Generated(value = "org.openapitools.codegen.languages.JavaClientCodegen", comments = "Generator version: 7.13.0")
public class ThreadStack {
  public static final String JSON_PROPERTY_THREAD_ID = "threadId";
  @javax.annotation.Nonnull
  private Long threadId;

  public static final String JSON_PROPERTY_THREAD_NAME = "threadName";
  @javax.annotation.Nonnull
  private String threadName;

  public static final String JSON_PROPERTY_THREAD_STATE = "threadState";
  @javax.annotation.Nonnull
  private String threadState;

  public static final String JSON_PROPERTY_STACK_TRACE = "stackTrace";
  @javax.annotation.Nonnull
  private List<String> stackTrace = new ArrayList<>();

  public static final String JSON_PROPERTY_BLOCKED_BY_THREAD_ID = "blockedByThreadId";
  @javax.annotation.Nullable
  private Long blockedByThreadId;

  public static final String JSON_PROPERTY_BLOCKED_BY_LOCK = "blockedByLock";
  @javax.annotation.Nullable
  private String blockedByLock;

  public static final String JSON_PROPERTY_HOLDING_LOCKS = "holdingLocks";
  @javax.annotation.Nullable
  private List<String> holdingLocks = new ArrayList<>();

  public static final String JSON_PROPERTY_SYNCHRONIZERS = "synchronizers";
  @javax.annotation.Nullable
  private List<String> synchronizers = new ArrayList<>();

  public static final String JSON_PROPERTY_MONITORS = "monitors";
  @javax.annotation.Nullable
  private List<String> monitors = new ArrayList<>();

  public static final String JSON_PROPERTY_LOCK_NAME = "lockName";
  @javax.annotation.Nullable
  private String lockName;

  public static final String JSON_PROPERTY_LOCK_OWNER_NAME = "lockOwnerName";
  @javax.annotation.Nullable
  private String lockOwnerName;

  public static final String JSON_PROPERTY_SUSPENDED = "suspended";
  @javax.annotation.Nullable
  private Boolean suspended;

  public static final String JSON_PROPERTY_IN_NATIVE = "inNative";
  @javax.annotation.Nullable
  private Boolean inNative;

  public ThreadStack() {
  }

  public ThreadStack threadId(@javax.annotation.Nonnull Long threadId) {
    
    this.threadId = threadId;
    return this;
  }

  /**
   * The id of the thread.
   * @return threadId
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_THREAD_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public Long getThreadId() {
    return threadId;
  }


  @JsonProperty(JSON_PROPERTY_THREAD_ID)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setThreadId(@javax.annotation.Nonnull Long threadId) {
    this.threadId = threadId;
  }

  public ThreadStack threadName(@javax.annotation.Nonnull String threadName) {
    
    this.threadName = threadName;
    return this;
  }

  /**
   * The name of the thread.
   * @return threadName
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_THREAD_NAME)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getThreadName() {
    return threadName;
  }


  @JsonProperty(JSON_PROPERTY_THREAD_NAME)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setThreadName(@javax.annotation.Nonnull String threadName) {
    this.threadName = threadName;
  }

  public ThreadStack threadState(@javax.annotation.Nonnull String threadState) {
    
    this.threadState = threadState;
    return this;
  }

  /**
   * The state of the thread.
   * @return threadState
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_THREAD_STATE)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public String getThreadState() {
    return threadState;
  }


  @JsonProperty(JSON_PROPERTY_THREAD_STATE)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setThreadState(@javax.annotation.Nonnull String threadState) {
    this.threadState = threadState;
  }

  public ThreadStack stackTrace(@javax.annotation.Nonnull List<String> stackTrace) {
    
    this.stackTrace = stackTrace;
    return this;
  }

  public ThreadStack addStackTraceItem(String stackTraceItem) {
    if (this.stackTrace == null) {
      this.stackTrace = new ArrayList<>();
    }
    this.stackTrace.add(stackTraceItem);
    return this;
  }

  /**
   * The stacktrace of the thread.
   * @return stackTrace
   */
  @javax.annotation.Nonnull
  @JsonProperty(JSON_PROPERTY_STACK_TRACE)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)

  public List<String> getStackTrace() {
    return stackTrace;
  }


  @JsonProperty(JSON_PROPERTY_STACK_TRACE)
  @JsonInclude(value = JsonInclude.Include.ALWAYS)
  public void setStackTrace(@javax.annotation.Nonnull List<String> stackTrace) {
    this.stackTrace = stackTrace;
  }

  public ThreadStack blockedByThreadId(@javax.annotation.Nullable Long blockedByThreadId) {
    
    this.blockedByThreadId = blockedByThreadId;
    return this;
  }

  /**
   * The id of the thread that the current thread is blocked by.
   * @return blockedByThreadId
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_BLOCKED_BY_THREAD_ID)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Long getBlockedByThreadId() {
    return blockedByThreadId;
  }


  @JsonProperty(JSON_PROPERTY_BLOCKED_BY_THREAD_ID)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setBlockedByThreadId(@javax.annotation.Nullable Long blockedByThreadId) {
    this.blockedByThreadId = blockedByThreadId;
  }

  public ThreadStack blockedByLock(@javax.annotation.Nullable String blockedByLock) {
    
    this.blockedByLock = blockedByLock;
    return this;
  }

  /**
   * The lock that the current thread is blocked by.
   * @return blockedByLock
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_BLOCKED_BY_LOCK)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getBlockedByLock() {
    return blockedByLock;
  }


  @JsonProperty(JSON_PROPERTY_BLOCKED_BY_LOCK)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setBlockedByLock(@javax.annotation.Nullable String blockedByLock) {
    this.blockedByLock = blockedByLock;
  }

  public ThreadStack holdingLocks(@javax.annotation.Nullable List<String> holdingLocks) {
    
    this.holdingLocks = holdingLocks;
    return this;
  }

  public ThreadStack addHoldingLocksItem(String holdingLocksItem) {
    if (this.holdingLocks == null) {
      this.holdingLocks = new ArrayList<>();
    }
    this.holdingLocks.add(holdingLocksItem);
    return this;
  }

  /**
   * The locks that the current thread is holding.
   * @return holdingLocks
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_HOLDING_LOCKS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<String> getHoldingLocks() {
    return holdingLocks;
  }


  @JsonProperty(JSON_PROPERTY_HOLDING_LOCKS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setHoldingLocks(@javax.annotation.Nullable List<String> holdingLocks) {
    this.holdingLocks = holdingLocks;
  }

  public ThreadStack synchronizers(@javax.annotation.Nullable List<String> synchronizers) {
    
    this.synchronizers = synchronizers;
    return this;
  }

  public ThreadStack addSynchronizersItem(String synchronizersItem) {
    if (this.synchronizers == null) {
      this.synchronizers = new ArrayList<>();
    }
    this.synchronizers.add(synchronizersItem);
    return this;
  }

  /**
   * The ownable synchronizers locked by the thread.
   * @return synchronizers
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_SYNCHRONIZERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<String> getSynchronizers() {
    return synchronizers;
  }


  @JsonProperty(JSON_PROPERTY_SYNCHRONIZERS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setSynchronizers(@javax.annotation.Nullable List<String> synchronizers) {
    this.synchronizers = synchronizers;
  }

  public ThreadStack monitors(@javax.annotation.Nullable List<String> monitors) {
    
    this.monitors = monitors;
    return this;
  }

  public ThreadStack addMonitorsItem(String monitorsItem) {
    if (this.monitors == null) {
      this.monitors = new ArrayList<>();
    }
    this.monitors.add(monitorsItem);
    return this;
  }

  /**
   * The object monitors locked by the thread.
   * @return monitors
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_MONITORS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public List<String> getMonitors() {
    return monitors;
  }


  @JsonProperty(JSON_PROPERTY_MONITORS)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setMonitors(@javax.annotation.Nullable List<String> monitors) {
    this.monitors = monitors;
  }

  public ThreadStack lockName(@javax.annotation.Nullable String lockName) {
    
    this.lockName = lockName;
    return this;
  }

  /**
   * The string representation of the object on which the thread is blocked if any.
   * @return lockName
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LOCK_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getLockName() {
    return lockName;
  }


  @JsonProperty(JSON_PROPERTY_LOCK_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLockName(@javax.annotation.Nullable String lockName) {
    this.lockName = lockName;
  }

  public ThreadStack lockOwnerName(@javax.annotation.Nullable String lockOwnerName) {
    
    this.lockOwnerName = lockOwnerName;
    return this;
  }

  /**
   * The name of the thread that owns the object this thread is blocked on.
   * @return lockOwnerName
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_LOCK_OWNER_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public String getLockOwnerName() {
    return lockOwnerName;
  }


  @JsonProperty(JSON_PROPERTY_LOCK_OWNER_NAME)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setLockOwnerName(@javax.annotation.Nullable String lockOwnerName) {
    this.lockOwnerName = lockOwnerName;
  }

  public ThreadStack suspended(@javax.annotation.Nullable Boolean suspended) {
    
    this.suspended = suspended;
    return this;
  }

  /**
   * Whether the thread is suspended.
   * @return suspended
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_SUSPENDED)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Boolean getSuspended() {
    return suspended;
  }


  @JsonProperty(JSON_PROPERTY_SUSPENDED)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setSuspended(@javax.annotation.Nullable Boolean suspended) {
    this.suspended = suspended;
  }

  public ThreadStack inNative(@javax.annotation.Nullable Boolean inNative) {
    
    this.inNative = inNative;
    return this;
  }

  /**
   * Whether the thread is executing native code.
   * @return inNative
   */
  @javax.annotation.Nullable
  @JsonProperty(JSON_PROPERTY_IN_NATIVE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)

  public Boolean getInNative() {
    return inNative;
  }


  @JsonProperty(JSON_PROPERTY_IN_NATIVE)
  @JsonInclude(value = JsonInclude.Include.USE_DEFAULTS)
  public void setInNative(@javax.annotation.Nullable Boolean inNative) {
    this.inNative = inNative;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ThreadStack threadStack = (ThreadStack) o;
    return Objects.equals(this.threadId, threadStack.threadId) &&
        Objects.equals(this.threadName, threadStack.threadName) &&
        Objects.equals(this.threadState, threadStack.threadState) &&
        Objects.equals(this.stackTrace, threadStack.stackTrace) &&
        Objects.equals(this.blockedByThreadId, threadStack.blockedByThreadId) &&
        Objects.equals(this.blockedByLock, threadStack.blockedByLock) &&
        Objects.equals(this.holdingLocks, threadStack.holdingLocks) &&
        Objects.equals(this.synchronizers, threadStack.synchronizers) &&
        Objects.equals(this.monitors, threadStack.monitors) &&
        Objects.equals(this.lockName, threadStack.lockName) &&
        Objects.equals(this.lockOwnerName, threadStack.lockOwnerName) &&
        Objects.equals(this.suspended, threadStack.suspended) &&
        Objects.equals(this.inNative, threadStack.inNative);
  }

  @Override
  public int hashCode() {
    return Objects.hash(threadId, threadName, threadState, stackTrace, blockedByThreadId, blockedByLock, holdingLocks, synchronizers, monitors, lockName, lockOwnerName, suspended, inNative);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("class ThreadStack {\n");
    sb.append("    threadId: ").append(toIndentedString(threadId)).append("\n");
    sb.append("    threadName: ").append(toIndentedString(threadName)).append("\n");
    sb.append("    threadState: ").append(toIndentedString(threadState)).append("\n");
    sb.append("    stackTrace: ").append(toIndentedString(stackTrace)).append("\n");
    sb.append("    blockedByThreadId: ").append(toIndentedString(blockedByThreadId)).append("\n");
    sb.append("    blockedByLock: ").append(toIndentedString(blockedByLock)).append("\n");
    sb.append("    holdingLocks: ").append(toIndentedString(holdingLocks)).append("\n");
    sb.append("    synchronizers: ").append(toIndentedString(synchronizers)).append("\n");
    sb.append("    monitors: ").append(toIndentedString(monitors)).append("\n");
    sb.append("    lockName: ").append(toIndentedString(lockName)).append("\n");
    sb.append("    lockOwnerName: ").append(toIndentedString(lockOwnerName)).append("\n");
    sb.append("    suspended: ").append(toIndentedString(suspended)).append("\n");
    sb.append("    inNative: ").append(toIndentedString(inNative)).append("\n");
    sb.append("}");
    return sb.toString();
  }

  /**
   * Convert the given object to string with each line indented by 4 spaces
   * (except the first line).
   */
  private String toIndentedString(Object o) {
    if (o == null) {
      return "null";
    }
    return o.toString().replace("\n", "\n    ");
  }

}

