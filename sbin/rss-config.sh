#!/usr/bin/env bash
unset HADOOP_CONF_DIR

# included in all the rss scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# symlink and absolute path should rely on RSS_HOME to resolve
if [ -z "${RSS_HOME}" ]; then
  export RSS_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

export RSS_CONF_DIR="${RSS_CONF_DIR:-"${RSS_HOME}/conf"}"

if [ -z "$RSS_ENV_LOADED" ]; then
  export RSS_ENV_LOADED=1

  if [ -f "${RSS_CONF_DIR}/rss-env.sh" ]; then
    # Promote all variable declarations to environment (exported) variables
    set -a
    . "${RSS_CONF_DIR}/rss-env.sh"
    set +a
  fi
fi
