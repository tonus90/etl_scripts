FROM ubuntu:18.04

RUN apt-get update -y && \
    apt-get install -y \
    vim \
    wget \
    ssh \
    less \
    unzip \
    sudo \
    python3-pip

RUN useradd -m user1 && \
    adduser user1 sudo  && \
    echo "user1 ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

RUN python3 -m pip install --upgrade pip

USER user1

WORKDIR /home/user1

ENV AIRFLOW_HOME /home/user1/airflow
ENV AIRFLOW_VERSION 2.1.3
ENV PYTHON_VERSION 3.6
ENV CONSTRAINT_URL "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

ENV PATH $PATH:/home/user1/.local/bin

RUN python3 -m pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

EXPOSE 8080

RUN airflow -h
RUN mkdir `airflow config get-value core dags_folder`
RUN airflow db init

ENV AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL 30

RUN airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin

ENV TZ=UTC

COPY my_wrapper_script.sh $AIRFLOW_HOME/my_wrapper_script.sh
RUN sudo chmod +x $AIRFLOW_HOME/my_wrapper_script.sh
CMD $AIRFLOW_HOME/my_wrapper_script.sh