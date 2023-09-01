FROM atddocker/atd-oracle-py:production

# Copy our own application
WORKDIR /app
COPY . /app

RUN chmod -R 755 /app/*

# # Proceed to install the requirements...do
RUN apt-get --allow-releaseinfo-change update
RUN apt-get install -y build-essential
RUN pip install -r requirements.txt
