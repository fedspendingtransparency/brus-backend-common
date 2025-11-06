FROM python:3.12

RUN apt-get -y update
RUN apt-get install -y libpq-dev
RUN apt-get install -y postgresql-client
RUN apt-get install -y netcat-openbsd
RUN apt-get install -y libsqlite3-dev
RUN apt-get install -y build-essential

RUN pip install unittest-xml-reporting setuptools==68.1.2

COPY requirements.txt /data-act/backend/requirements.txt

RUN pip install --upgrade pip==24.0
RUN pip install -r /data-act/backend/requirements.txt

ENV PYTHONPATH /data-act/backend
WORKDIR /data-act/backend

VOLUME /data-act/backend
ADD . /data-act/backend

CMD /bin/sh
