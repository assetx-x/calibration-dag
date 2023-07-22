FROM registry.marathon.mesos:5000/dcm-intuition-env:master

ARG git_user=dcmautosys
ARG git_user_name="Security Universe Pipeline"
ARG git_user_email="autosys@datacapitalmanagement.com"

ARG git_pwd

ARG git_branch=master

ADD ./ /opt/data_processing

RUN cd /opt && \
    git clone https://${git_user}:"${git_pwd}"@github.com/datacapitalmanagement/dcm-config.git && \
    git config --global push.default simple && \
    git config --global user.name "${git_user_name}" && \
    git config --global user.email "${git_user_email}" && \
    cd dcm-config && \
    git checkout ${git_branch}

WORKDIR /opt/data_processing
