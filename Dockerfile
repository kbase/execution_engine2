FROM kbase/sdkbase2:python
MAINTAINER KBase Developer
RUN apt-get update
# -----------------------------------------
# In this section, you can install any system dependencies required
# to run your App.  For instance, you could place an apt-get update or
# install line here, a git checkout to download code, or run any other
# installation scripts.
RUN apt-get install -y gcc wget vim

RUN DEBIAN_FRONTEND=noninteractive wget -qO - https://research.cs.wisc.edu/htcondor/debian/HTCondor-Release.gpg.key | apt-key add - \
    && echo "deb http://research.cs.wisc.edu/htcondor/debian/8.8/stretch stretch contrib" >> /etc/apt/sources.list \
    && echo "deb-src http://research.cs.wisc.edu/htcondor/debian/8.8/stretch stretch contrib" >> /etc/apt/sources.list \
    && apt-get update -y \
    && apt-get install -y condor

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



COPY ./requirements.txt /kb/module/requirements.txt
RUN pip install -r /kb/module/requirements.txt
RUN useradd kbase

# -----------------------------------------

COPY ./ /kb/module
RUN mkdir -p /kb/module/work && chmod -R a+rw /kb/module && mkdir -p /etc/condor/


WORKDIR /kb/module
RUN make all

WORKDIR /kb/module/scripts
RUN chmod +x download_runner.sh && ./download_runner.sh

WORKDIR /kb/module/

ENTRYPOINT [ "./scripts/entrypoint.sh" ]
CMD [ ]

