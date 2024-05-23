# FROM tiangolo/uvicorn-gunicorn:python3.11-slim
FROM python:3.10


# The env_param is set to production
ENV env_param="production"

# Create FastAPI working directory a
RUN mkdir /data-fastapi



# This is a copy of  current directory to  data-fastapi directory
COPY . /data-fastapi

# Change working directory
WORKDIR /data-fastapi

# Install all the required libraries
RUN pip install -r requirements.txt
# Expose API application to port 8081
EXPOSE 8081

# Run the application
CMD uvicorn data_api.main:app --proxy-headers --host 0.0.0.0 --port 8081