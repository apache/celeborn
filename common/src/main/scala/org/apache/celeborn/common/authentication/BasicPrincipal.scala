package org.apache.celeborn.common.authentication

import java.security.Principal
import java.util.Objects

class BasicPrincipal(name: String) extends Principal {
  Objects.requireNonNull(name, "Principal name cannot be null")
  override def getName: String = name

  override def toString: String = name

  override def hashCode(): Int = Objects.hash(name)

  override def equals(o: Any): Boolean = {
    if (this == o) {
      true
    } else if (o == null || getClass != o.getClass) {
      false
    } else {
      Objects.equals(name, o.asInstanceOf[BasicPrincipal].name)
    }
  }
}
