# The Dockerfile is currently buggy and breaks on the java install, This will require more exploration
FROM ubuntu:18.04

# Install Java
RUN apt-get update \
    && apt-get install -y software-properties-common \
    && add-apt-repository -y ppa:linuxuprising/java \
    && apt-get -y clean \
    && apt-get -y update \
    && apt-get -y upgrade \
    && apt-get install -f \
    && apt install -y oracle-java15-installer

# Install Python and pip
RUN apt-get install python3 \
    && python3 install pip

# Create app directory
WORKDIR /app

# Install app dependencies
COPY requirements.txt ./

RUN pip install -r requirements.txt

# Bundle app source
COPY src /app

CMD [ "python", "pyspark_pipeline.py" ]