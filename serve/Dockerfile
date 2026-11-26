# Plain-Python image for Part 3 (database loaders and queries) and the stream consumer.
# Libraries are installed once when the image is built, not every time a container starts.
FROM python:3.13.17-slim
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
WORKDIR /work
