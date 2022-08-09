FROM centos:7.6.1810

RUN yum install -y java-1.8.0-openjdk-devel sshpass openssh-clients vim epel-release bind-utils \
    supervisor telnet net-tools procps && \
    yum clean all && rm -rf /var/cache/yum

ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

RUN mkdir -p /opt/
ADD rss-0.1.1-bin-release.tar.gz /opt/
