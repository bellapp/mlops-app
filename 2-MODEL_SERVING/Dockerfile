# using ultralytics/yolov5 template
FROM ultralytics/yolov5:latest-cpu



ENV WORKERS_PER_CORE=4 
ENV MAX_WORKERS=24
ENV LOG_LEVEL="warning"
ENV TIMEOUT="200"
ENV env_download_models="True"
RUN mkdir /yolov5-fastapi
RUN mkdir /emissions_codecarbon
COPY .codecarbon.config /root/.codecarbon.config


COPY requirements.txt /yolov5-fastapi


COPY . /yolov5-fastapi

WORKDIR /yolov5-fastapi


RUN pip install -r requirements.txt

EXPOSE 80


CMD uvicorn main:app --proxy-headers --host 0.0.0.0 --port 80