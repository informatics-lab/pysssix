FROM continuumio/miniconda3


RUN apt-get install libfuse-dev -y
RUN conda install -c bioconda -c anaconda -c conda-forge fusepy boto3 smart_open iris -y 

CMD ["/bin/bash"]
