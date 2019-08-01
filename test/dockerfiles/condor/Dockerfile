FROM centos/systemd:latest
ENV container docker

RUN yum update -y \
    && yum install -y https://centos7.iuscommunity.org/ius-release.rpm \
    && yum install -y python36u python36u-libs python36u-devel python36u-pip \
    && yum install -y which gcc  \
    && yum install wget -y \
    && wget https://research.cs.wisc.edu/htcondor/yum/RPM-GPG-KEY-HTCondor; rpm --import RPM-GPG-KEY-HTCondor;

RUN cd /etc/yum.repos.d && rm -rf *htcondor* \
    && wget https://research.cs.wisc.edu/htcondor/yum/repo.d/htcondor-stable-rhel7.repo \
    && wget https://research.cs.wisc.edu/htcondor/yum/repo.d/htcondor-development-rhel7.repo \
    && yum update; yum install -y minicondor

RUN yum install -y sudo passwd openssh-server openssh-clients
RUN pip3.6 install pipenv && pip3.6 install --upgrade pip

COPY requirements.txt requirements.txt
COPY start_condor.sh start_condor.sh

RUN pip install -r requirements.txt
RUN echo "root:root" | chpasswd
RUN useradd kbase && echo "kbase:kbase" | chpasswd

CMD ["/usr/sbin/init"]

# docker build -f Dockerfile . -t kbase/ee2:local_condor
# docker push kbase/ee2:local_condor
# docker run -ti -d --privileged=true -v /sys/fs/cgroup:/sys/fs/cgroup:ro -v /Users:/Users -p 22:22 --net=my_bridge ee2:local_condor
# docker run -ti -d  --net=my_bridge mongo:2.7
# docker network connect my_bridge 51ef0e863717