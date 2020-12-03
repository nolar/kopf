#!/bin/bash
# Install kubectl.
#
# Use the latest client version always, ignore the requested K8s version.
# Kubectl is not a system-under-tests, it is a environment configuring tool.
#
set -eu
set -x

: ${K8S:=latest}

if [[ "$K8S" == latest ]] ; then
    K8S="$( curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt )"
fi

curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/"$K8S"/bin/linux/amd64/kubectl
chmod +x kubectl
sudo mv kubectl /usr/local/bin/
