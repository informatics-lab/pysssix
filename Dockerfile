FROM continuumio/miniconda3


RUN apt-get install libfuse-dev -y
RUN conda install -c bioconda -c anaconda -c conda-forge fusepy boto3  diskcache -y 

CMD ["/bin/bash"]
