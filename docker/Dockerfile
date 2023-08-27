FROM debian:12
WORKDIR /usr/local/processing/dqm-pipeline/

# Local time zone
rm /etc/localtime
ln -s /usr/share/zoneinfo/Europe/Berlin /etc/localtime

RUN apt update

# System utilities
RUN apt install wget aria2 --yes

# Python 3.11
RUN apt install python3 --yes
RUN apt install python3-pip --yes

# Java 17
RUN apt install openjdk-17-jdk --yes
RUN export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/

# App requirements
RUN pip3 install bibtexparser --break-system-packages
RUN pip3 install pyspark --break-system-packages
RUN pip3 install pylatexenc --break-system-packages
