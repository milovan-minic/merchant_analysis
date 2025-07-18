FROM jupyter/pyspark-notebook:latest

# Copy requirements.txt into the image
COPY requirements.txt /tmp/requirements.txt

# Install any additional Python dependencies
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set the working directory to the notebook's work directory
WORKDIR /home/jovyan/app