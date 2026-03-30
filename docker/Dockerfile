FROM apache/spark:3.5.1-python3

USER root

# Cài thư viện Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Cấu hình Hadoop client để kết nối HDFS
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PYSPARK_PYTHON=python3

# Copy Hadoop config template
COPY hadoop-client.xml /opt/hadoop/etc/hadoop/core-site.xml

WORKDIR /app

# Mặc định chạy Jupyter
CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", \
     "--no-browser", "--allow-root", "--NotebookApp.token=''"]
