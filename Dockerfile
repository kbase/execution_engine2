FROM quay.io/kbase/sdkbase2:python
MAINTAINER KBase Developer

RUN apt-get clean all && apt-get update --fix-missing -y

# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.
RUN apt-get install -y gcc wget vim htop tmpreaper
RUN mkdir -p /etc/apt/sources.list.d


RUN DEBIAN_FRONTEND=noninteractive wget -qO - https://research.cs.wisc.edu/htcondor/debian/HTCondor-Release.gpg.key | apt-key add - \
    && echo "deb http://research.cs.wisc.edu/htcondor/debian/8.8/stretch stretch contrib" >> /etc/apt/sources.list \
    && echo "deb-src http://research.cs.wisc.edu/htcondor/debian/8.8/stretch stretch contrib" >> /etc/apt/sources.list \
    && apt-get update -y \
    && apt-get install -y condor

# install jars
# perhaps we should have test and prod dockerfiles to avoid jars and mongo installs in prod
RUN cd /opt \
    && git clone https://github.com/kbase/jars \
    && cd -
    
# Remove due to cve-2021-4104 issue in spin (log4j)
RUN rm /opt/jars/lib/jars/dockerjava/docker-java-shaded-3.0.14.jar


# install mongodb
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2930ADAE8CAF5059EE73BB4B58712A2291FA4AD5 \
    && echo "deb http://repo.mongodb.org/apt/debian stretch/mongodb-org/3.6 main" | tee /etc/apt/sources.list.d/mongodb-org-3.6.list  \
    && apt-get update \
    && apt-get install -y --no-install-recommends mongodb-org=3.6.11 mongodb-org-server=3.6.11 mongodb-org-shell=3.6.11 mongodb-org-mongos=3.6.11 mongodb-org-tools=3.6.11 \
    && apt-get install -y --no-install-recommends mongodb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN echo "mongodb-org hold" | dpkg --set-selections \
    && echo "mongodb-org-server hold" | dpkg --set-selections \
    && echo "mongodb-org-shell hold" | dpkg --set-selections \
    && echo "mongodb-org-mongos hold" | dpkg --set-selections \
    && echo "mongodb-org-tools hold" | dpkg --set-selections

#Install Python3 and Libraries (source /root/miniconda/bin/activate)
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh \
&& bash ~/miniconda.sh -b -p /miniconda-latest

# Setup Cron
COPY ./bin/ee2_cronjobs /etc/cron.d/ee2_cronjobs

# Need to change startup scripts to match this in MAKEFILE
ENV PATH=/miniconda-latest/bin:$PATH
RUN pip install --upgrade pip && python -V
COPY ./requirements.txt /kb/module/requirements.txt

RUN pip install -r /kb/module/requirements.txt
RUN adduser --disabled-password --gecos '' -shell /bin/bash kbase
# -----------------------------------------

COPY ./ /kb/module
RUN mkdir -p /kb/module/work && chmod -R a+rw /kb/module && mkdir -p /etc/condor/

WORKDIR /kb/module
RUN make all

WORKDIR /kb/module/scripts
RUN chmod +x download_runner.sh && ./download_runner.sh

WORKDIR /kb/module/



# Set deploy.cfg location
ENV KB_DEPLOYMENT_CONFIG=/kb/module/deploy.cfg
ENV PATH=/kb/module:$PATH

ENTRYPOINT [ "./scripts/entrypoint.sh" ]
CMD [ ]
