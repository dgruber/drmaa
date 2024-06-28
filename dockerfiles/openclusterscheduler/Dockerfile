FROM hpcgridware/clusterscheduler-latest-ubuntu2204:eac1ec597

RUN mkdir -p /opt/helpers

COPY autoinstall.template /opt/helpers/
COPY installer.sh /opt/helpers/
COPY entrypoint.sh /entrypoint.sh

ARG GOLANG_VERSION=1.22.4

RUN apt-get update && \
    apt-get install -y wget git gcc make vim libhwloc-dev hwloc

RUN wget https://go.dev/dl/go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    rm -rf /usr/local/go && \
    tar -C /usr/local -xzf go${GOLANG_VERSION}.linux-amd64.tar.gz && \
    rm go${GOLANG_VERSION}.linux-amd64.tar.gz

ENV PATH=$PATH:/usr/local/go/bin

RUN mkdir -p /root/go/bin && \
    mkdir -p /root/go/src/github.com/dgruber

RUN cd /root/go/src/github.com/dgruber && \
    git clone https://github.com/dgruber/drmaa.git

WORKDIR /root/go/src/github.com/dgruber/drmaa

ENV SGE_ROOT=/opt/cs-install
ENV LD_LIBRARY_PATH ${SGE_ROOT}/lib/lx-amd64

ENTRYPOINT [ "/entrypoint.sh" ]
