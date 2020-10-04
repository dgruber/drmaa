FROM giovtorres/docker-centos7-slurm:latest

RUN yum groupinstall -y 'Development Tools'

RUN yum install -y gperf
RUN yum install -y ragel
RUN yum install -y automake

RUN git clone --recursive https://github.com/natefoo/slurm-drmaa.git
RUN cd slurm-drmaa && ./autogen.sh && ./configure && make && make install

RUN wget https://golang.org/dl/go1.15.2.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.15.2.linux-amd64.tar.gz
RUN echo 'export PATH=$PATH:/usr/local/go/bin' > /etc/profile
RUN source /etc/profile
RUN echo 'export PATH=$PATH:/usr/local/go/bin' >> /root/.bashrc
RUN mkdir -p /root/go/bin
RUN mkdir -p /root/go/src/github.com/dgruber

RUN cd /root/go/src/github.com/dgruber && git clone https://github.com/dgruber/drmaa.git


