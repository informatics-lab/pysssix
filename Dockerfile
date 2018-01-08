FROM continuumio/miniconda3


RUN apt-get install libfuse-dev -y
RUN conda install -c bioconda -c anaconda -c conda-forge fusepy boto3  diskcache -y 
RUN conda install jupyter -y 
EXPOSE 7766
CMD ["/bin/bash"]
