FROM alpine:latest
# Create the directory
RUN mkdir -p /usr/share/logstash/pipeline
# Copy the config from their repo into the image
COPY monitoring_elk/logstash/pipeline/logstash.conf /usr/share/logstash/pipeline/logstash.conf
# Keep the container alive or just use it as a volume source
CMD ["tail", "-f", "/dev/null"]