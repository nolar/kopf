#!/bin/bash
# Install K8s via KinD (Kubernetes-in-Docker).
#
# Spin-up times previously detected:
# * k3d -- 20 seconds.
# * kind -- 90 seconds.
# * minikube -- 110-120 seconds.
#
# Not all of the latest K8s versions are available as the Kind versions.
# Care should be taken when upgrading. Check the available versions at:
# https://hub.docker.com/r/kindest/node/tags
#
set -eu
set -x

: ${KIND:=latest}
: ${K8S:=latest}
if [[ "$K8S" == latest ]] ; then
    K8S="$( curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt )"
fi

curl -Lo ./kind https://kind.sigs.k8s.io/dl/"$KIND"/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/

kind create cluster --image=kindest/node:"$K8S"
