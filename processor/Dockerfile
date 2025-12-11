FROM python:3.9-slim-bookworm

# Install Java and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY processor.py .

CMD ["python", "-u", "processor.py"]
