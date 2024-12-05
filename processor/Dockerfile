FROM bde2020/spark-base:3.3.0-hadoop3.3

# Set working directory
WORKDIR /app

# Install Python and dependencies
RUN apk add --no-cache python3 py3-pip && \
    pip3 install kafka-python six pyspark

# Copy the processing script
COPY processor.py /app/

# Command to run the Spark job
CMD ["/spark/bin/spark-submit", "--master", "local[*]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", "/app/processor.py"]
