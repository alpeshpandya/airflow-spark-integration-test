FROM singularities/spark
RUN apt-get update
RUN apt-get install  -y ssh
RUN service ssh start
RUN mkdir ~/.ssh
RUN cat /dev/zero | ssh-keygen -q -N ""
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod og-wx ~/.ssh/authorized_keys
RUN sed -i '25s/.*/export JAVA_HOME=\/docker-java-home/' /usr/local/hadoop-2.8.2/etc/hadoop/hadoop-env.sh
RUN apt install -y build-essential checkinstall
RUN apt install -y libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev
RUN wget https://www.python.org/ftp/python/3.6.0/Python-3.6.0.tar.xz
RUN tar xvf Python-3.6.0.tar.xz
RUN cd Python-3.6.0 && ./configure && make && make altinstall && cd ..
