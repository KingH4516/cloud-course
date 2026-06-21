FROM bitnami/spark:3.4.0
USER root
RUN apt-get update && apt-get install -y wget
RUN wget -P /opt/bitnami/spark/jars/ https://repo.huaweicloud.com/repository/maven/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/bitnami/spark/jars/ https://repo.huaweicloud.com/repository/maven/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
USER 1001