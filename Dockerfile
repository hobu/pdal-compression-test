FROM pdal/pdal:1.2
MAINTAINER Howard Butler <howard@hobu.co>

ENV CC clang
ENV CXX clang++

RUN apt-get update && apt-get install -y --fix-missing --no-install-recommends \
        libjsoncpp-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install awscli boto3


RUN git clone https://github.com/hobu/pdal-compression-test.git \
    && cd /pdal-compression-test \
	&& make \
	&& make install
