#!/bin/bash
set -eu
set -x

if [[ "${CLIENT:-}" = "yes" ]] ; then
    # FIXME: See https://github.com/kubernetes-client/python/issues/866
    pip install 'kubernetes<10.0.0'
fi
