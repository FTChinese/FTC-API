#!/usr/bin/env bash

if [ -z "${EASY_HOME}" ]; then
  export EASY_HOME="$(cd "`dirname "$0"`"/..; pwd)"
fi

if [ -z "${EASY_CONF_DIR}" ]; then
  export EASY_CONF_DIR="$EASY_HOME/conf"
fi

if [ -f "${EASY_CONF_DIR}/easy-env.sh" ]; then
# Promote all variable declarations to environment (exported) variables
set -a
. "${EASY_CONF_DIR}/easy-env.sh"
set +a
fi
