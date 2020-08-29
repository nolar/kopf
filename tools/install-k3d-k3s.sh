#!/bin/bash
# Install and run a version of minified K8s as K3s via K3d (K3s-in-Docker).
#
# Note: for the latest version, use the latest version of K3s, not of K8s:
# the latest K8s is useless unless there is a corresponding K3s version.
#
# Care should be taken when upgrading. Check the available versions at:
# https://hub.docker.com/r/rancher/k3s/tags
#
# Ignore hotfixes for now (e.g. k3s2, k3s3), as it is difficult to detect
# the latest one (there are no tags), and they are rare.
#
set -eu
set -x

: ${K3S:=latest}

curl -s https://raw.githubusercontent.com/rancher/k3d/main/install.sh | bash
k3d cluster create --wait --no-lb --image=rancher/k3s:"${K3S//+/-}"

# Sometimes, the service account is not created immediately. Nice trick, but no:
# we need to wait until the cluster is fully ready before starting the tests.
while ! kubectl get serviceaccount default >/dev/null; do sleep 1; done
