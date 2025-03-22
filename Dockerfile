# Use a base image with Python
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy the project files to the container
COPY . /app

# Run the install script
RUN sh install.sh

# Keep the container running
CMD ["tail", "-f", "/dev/null"]