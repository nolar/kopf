#!/bin/bash
# See https://github.com/LiliC/travis-minikube for Travis+Minikube.
set -eu
set -x

: ${KUBERNETES_VERSION:=latest}
: ${MINIKUBE_VERSION:=latest}

if [[ "${KUBERNETES_VERSION}" == latest ]] ; then
    KUBERNETES_VERSION=$( curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt )
fi

curl -Lo kubectl https://storage.googleapis.com/kubernetes-release/release/"${KUBERNETES_VERSION}"/bin/linux/amd64/kubectl
chmod +x kubectl
sudo mv kubectl /usr/local/bin/

curl -Lo minikube https://storage.googleapis.com/minikube/releases/"${MINIKUBE_VERSION}"/minikube-linux-amd64
chmod +x minikube
sudo mv minikube /usr/local/bin/

mkdir -p $HOME/.kube $HOME/.minikube
touch $KUBECONFIG

sudo minikube start \
    --vm-driver=none \
    --extra-config=apiserver.authorization-mode=RBAC \
    --extra-config=apiserver.runtime-config=events.k8s.io/v1beta1=false \
    --kubernetes-version="${KUBERNETES_VERSION}"

sudo chown -R travis: /home/travis/.minikube/
