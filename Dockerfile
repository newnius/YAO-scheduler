FROM ubuntu:16.04

RUN apt update && \
	apt install -y wget

RUN wget https://dl.google.com/go/go1.12.4.linux-amd64.tar.gz && \
	tar -C /usr/local -xzf go1.12.4.linux-amd64.tar.gz && \
	rm go1.12.4.linux-amd64.tar.gz

ENV PATH $PATH:/usr/local/go/bin