FROM ubuntu:16.04

RUN apt-get -y -q update && \
    apt-get -y -q install openssh-server \
                          iptables \
                          ntpdate \
                          sudo \
                          curl

# Enable ssh server; from http://docs.docker.com/engine/examples/running_ssh_service/
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
