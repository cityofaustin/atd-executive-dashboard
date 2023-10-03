FROM --platform=linux/x86_64 atddocker/atd-oracle-py:production 

# Copy our own application
WORKDIR /app
COPY . /app

RUN chmod -R 755 /app/*

# # Proceed to install the requirements...do
RUN apt-get --allow-releaseinfo-change update

# Installing conda and geopandas
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh
RUN bash ~/miniconda.sh -b -p $HOME/miniconda
ENV PATH="/root/miniconda/bin:${PATH}"
RUN conda install --channel conda-forge geopandas

RUN pip install -r requirements.txt
