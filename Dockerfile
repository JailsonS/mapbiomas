# syntax=docker/dockerfile:1

FROM ubuntu:20.04

WORKDIR /project

COPY requirements.txt ./

COPY mapbiomas ./

COPY credentials/joao /root/.config/earthengine/joao/
COPY credentials/mapbiomas1 /root/.config/earthengine/mapbiomas1/

COPY credentials/joao/credentials /root/.config/earthengine/

COPY ./script.sh /

RUN chmod +x /script.sh

RUN apt-get update && \
    apt-get install -y software-properties-common

RUN apt-get update && \
    apt-get -y install python3-pip

RUN apt -y install curl

RUN apt -y install git

RUN apt-get -y install zip

RUN apt-get -y install gdal-bin && \
    apt-get -y install libgdal-dev

RUN apt-get update && \
    add-apt-repository ppa:ubuntugis/ppa && \
    export CPLUS_INCLUDE_PATH=/usr/include/gdal && \
    export C_INCLUDE_PATH=/usr/include/gdal && \
    pip install GDAL

RUN echo \
    "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.gpg && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y


RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

# ENTRYPOINT ["/script.sh"]
# CMD ["script.sh", "joao"]