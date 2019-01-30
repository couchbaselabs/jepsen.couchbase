FROM ubuntu:16.04

ENV LEIN_ROOT true

# Dependencies

RUN apt-get -y -q update && \
    apt-get install -qqy --no-install-recommends \
        openjdk-8-jre-headless \
	openssh-server \
	openssh-client \
        gnuplot-nox \
        wget \
        curl

RUN wget https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein && \
    mv lein /usr/bin && \
    chmod +x /usr/bin/lein && \
    lein self-install

# Add jepsen to image
ADD jepsen /jepsen
RUN cd /jepsen && lein deps

# We need a .ssh/known_hosts file or jepsen dies
RUN mkdir /root/.ssh
RUN touch /root/.ssh/known_hosts

# Enable ssh server; from http://docs.docker.com/engine/examples/running_ssh_service/
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd
RUN sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]
