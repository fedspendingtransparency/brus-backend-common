ARG PYTHON_VERSION
FROM python:${PYTHON_VERSION}

ARG JAVA_VERSION
ARG HADOOP_VERSION
ARG SPARK_VERSION
ARG PROJECT_LOG_DIR=/logs
ARG DOWNLOAD_JARS=false

COPY --from=ghcr.io/astral-sh/uv:0.7.19 /uv /uvx /bin/

ENV PYTHONPATH /project
WORKDIR /project

VOLUME /project
ADD . /project

RUN apt update && apt install -y \
    build-essential \
    coreutils \
    curl \
    gcc \
    libbz2-dev \
    libpq-dev \
    libssl-dev \
    libsqlite3-dev \
    netcat-openbsd \
    openssl \
    postgresql-client \
    procps \
    wget

# Install Amazon Corretto (a Long-Term Supported (LTS) distribution of OpenJDK)
RUN wget -qO - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list
RUN apt update && \
    apt install -y java-${JAVA_VERSION}-amazon-corretto-jdk
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-amazon-corretto

# Install Hadoop and Spark into the image
# Commented out as pyspark + config "spark.jars.packages" installs them much faster. Uncomment if that approach becomes unreliable.
#
# WORKDIR /usr/local
#
#RUN wget --quiet https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
#    && tar xzf hadoop-${HADOOP_VERSION}.tar.gz \
#    && ln -sfn /usr/local/hadoop-${HADOOP_VERSION} /usr/local/hadoop \
#    && wget --quiet https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
#    && tar xzf spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
#    && ln -sfn /usr/local/spark-${SPARK_VERSION}-bin-without-hadoop /usr/local/spark \
#    && echo "Installed $(/usr/local/hadoop/bin/hadoop version)"
#ENV HADOOP_HOME=/usr/local/hadoop
#ENV SPARK_HOME=/usr/local/spark
## Cannot set ENV var = command-result, [i.e. doing: ENV SPARK_DIST_CLASSPATH=$(${HADOOP_HOME}/bin/hadoop classpath)], so interpolating the hadoop classpath the long way
#ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
#ENV PATH=${SPARK_HOME}/bin:${HADOOP_HOME}/bin:${JAVA_HOME}/bin:${PATH}
#RUN echo "Installed Spark" && echo "$(${SPARK_HOME}/bin/pyspark --version)"

WORKDIR /project

##### The following ENV vars are optimizations from https://github.com/astral-sh/uv-docker-example/blob/main/Dockerfile
##### and https://docs.astral.sh/uv/guides/integration/docker/#optimizations
# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Use the system Python environment since the container is already isolated
ENV UV_PROJECT_ENVIRONMENT=/usr/local
ENV UV_SYSTEM_PYTHON=1

RUN uv pip install unittest-xml-reporting setuptools==68.1.2
RUN uv pip install --upgrade pip==24.0

# Install dependencies
# Including dev too as this repo will not be hosting an API server
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --extra awscli --extra dev --extra spark --locked --no-install-project

# Download the spark jars and stored them in the image (/root/.ivy2), primarily to save time for github actions
RUN if [ "${DOWNLOAD_JARS}" == "true" ]; then \
    pytest --numprocesses logical --no-cov --disable-warnings -r=fEs --verbosity=3 \
    "brus_backend_common/tests/integration/test_setup_of_spark_dependencies.py::test_preload_spark_jars" ; \
    fi

CMD /bin/sh
