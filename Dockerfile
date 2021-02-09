FROM continuumio/miniconda3

WORKDIR /workspace/data

RUN apt-get update && apt-get install -y python-pip && \
    apt-get install -y unixodbc unixodbc-dev && \
    apt-get install -y wget && rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash", "-c"]


COPY . /workspace/data

RUN conda update -n base -c defaults conda && \ 
    conda config --set channel_priority strict && \
    conda env create -f /workspace/data/environment.yaml && \
    conda --version

RUN echo "source activate ihs" > ~/.bashrc
ENV PATH /opt/conda/envs/ihs/bin:$PATH

