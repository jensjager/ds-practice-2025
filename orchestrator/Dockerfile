# Use an official Python runtime as the base image
FROM python:3.11

# Set the working directory in the container
# Both the utils and src folders will be mounted as volumes, please see docker-compose.yaml
WORKDIR /app

# Set PYTHONPATH to include the app directory for absolute imports
ENV PYTHONPATH=/app:$PYTHONPATH

# Copy the requirements file to the working directory
COPY ./orchestrator/requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set the command to run the container
CMD python utils/other/hotreload.py "orchestrator/src/app.py"
