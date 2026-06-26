# Custom image based on the official Debezium connect image
# Adds the JDBC Sink plugin and the PostgreSQL driver.
FROM quay.io/debezium/connect:latest

RUN mkdir -p /kafka/connect/kafka-connect-jdbc

# JDBC Sink Connector
RUN curl -L https://repo1.maven.org/maven2/io/confluent/kafka-connect-jdbc/10.7.4/kafka-connect-jdbc-10.7.4.jar \
    -o /kafka/connect/kafka-connect-jdbc/kafka-connect-jdbc.jar

# PostgreSQL JDBC Driver
RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -o /kafka/connect/kafka-connect-jdbc/postgresql.jar
