FROM nextflow/nextflow:latest

# Install make using yum, clean up cache, and remove unnecessary files in a single RUN
RUN yum install -y make && \
    yum clean all && \
    rm -rf /var/cache/yum

# Set HOME to root directory
ENV HOME=/

# Copy only necessary files to reduce image size
COPY . .

# Run make install
RUN make install-in-container && \
    # Cleanup unnecessary files after installation
    rm -rf /tmp/* /var/tmp/*

# Use the existing entrypoint defined in the base image
ENTRYPOINT ["/usr/local/bin/entry.sh"]
