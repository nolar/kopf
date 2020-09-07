#!/bin/bash
# Install K8s via Minikube.
#
# Minikube is heavy, but reliable, can run almost any version of K8s.
# Spin-up times previously detected:
# * k3d -- 20 seconds.
# * kind -- 90 seconds.
# * minikube -- 110-120 seconds.
#
# Based on https://github.com/LiliC/travis-minikube.
#
set -eu
set -x

: ${K8S:=latest}
if [[ "$K8S" == latest ]] ; then
    K8S="$( curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt )"
fi

curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
chmod +x minikube
sudo mv minikube /usr/local/bin/

mkdir -p $HOME/.kube $HOME/.minikube
touch $HOME/.kube/config

sudo apt-get update -y
sudo apt-get install -y conntrack  # see #334

minikube config set driver docker
minikube start \
    --extra-config=apiserver.authorization-mode=Node,RBAC \
    --extra-config=apiserver.runtime-config=events.k8s.io/v1beta1=false \
    --kubernetes-version="$K8S"
