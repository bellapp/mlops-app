from fastapi import FastAPI, File, HTTPException, Response, UploadFile
from fastapi.responses import JSONResponse
import apihelper
from starlette.responses import Response
import io
from PIL import Image
import json
from fastapi.middleware.cors import CORSMiddleware
import sys
import boto3
import os
import cv2
import numpy as np
from codecarbon import EmissionsTracker
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from sqlalchemy import create_engine
from codecarbon.output import BaseOutput
from datetime import datetime
from pydantic_settings import BaseSettings
import base64

# The `app = FastAPI(...)` statement is creating a new instance of the FastAPI framework with specific
# parameters:
app = FastAPI(
    title="Custom YOLOV5 Machine Learning API",
    description="Obtain object value out of image and return image and JSON result",
    version="1.0.1",
)

# This class `DatabaseOutput` inherits from `BaseOutput` and is used to insert emissions data into a
# MySQL table.
class DatabaseOutput(BaseOutput):
    """
    Class inheriting from BaseOutput codecarbon class, to insert data into a MySQL table.
    """
    def __init__(self, db_uri, table_name):
        self.engine = create_engine(db_uri)
        self.table_name = table_name
      
    def out(self, data: float):
        """
        Convert the emissions data to a DataFrame and load it into the database table.
        """
        emission_data = {
            'timestamp': [datetime.now()],
            'source': ['microteksapi'],
            'emission': [data]
        }
        df = pd.DataFrame(emission_data)
        
        with self.engine.begin() as conn:
            df.to_sql(self.table_name, con=conn, if_exists='append', index=False)

# This class `CustomEmissionsTracker` extends `EmissionsTracker` to create a custom emissions tracker
# that uses `DatabaseOutput` for outputting data to a specified database table.
class CustomEmissionsTracker(EmissionsTracker):
    """
    Class to initiate a custom emissions tracker that uses DatabaseOutput.
    """
    def __init__(self, db_uri, table_name):
        super().__init__()
        self.output = DatabaseOutput(db_uri, table_name)

    def stop(self):
        data = super().stop()
        self.output.out(data)
        return data


# `DB_CONNECTION_STRING` which contains the connection string for a MySQL database

DB_CONNECTION_STRING = 'mysql+pymysql://ip_address/codecarbondb'
# `TABLE_NAME` which contains the name of a table in the codecarbon database.
TABLE_NAME = 'emission'

# FastAPI initial parameters
origins = [
    "http://localhost",
    "http://localhost:8000",
    "*"
]

#  adding a CORS middleware to the application. 

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# The class `Settings` defines configuration settings for an API with default values.
class Settings(BaseSettings):
    """
    The class `Settings` defines configuration settings for an API with default values.
    """
    app_name: str = "Awesome API"
    env_download_models: str = ""

# The above code is creating an instance of the `Settings` class and assigning it to the variable
# `settings`.
settings = Settings()

@app.on_event("startup")
async def startup_event():
    """
    The `startup_event` function checks a setting and downloads models based on the setting value.
    """
    print("startup function")
    print("settings.env_download_models =", settings.env_download_models)
    if settings.env_download_models == "False":
        apihelper.downloadmodels(is_download_file=False)
    else:
        apihelper.downloadmodels(is_download_file=True)

@app.get('/')
def get_health():
    """
    Return a message indicating API is ready to use.
    """
    return dict(msg=f'API Microtecks Is Great 66, {sys.version}')

@app.post("/predict")
async def detect(clientid: str, usecase: str, confidence: float = 0.45, nms: float = 0.25, image: bytes = File(...)) -> dict:
    """
     Run object detection on an image. This is a wrapper around the : py : func : ` ~emissions. apihelper. get_model ` function to get a specific model for a usecase and return information about the detected objects in the image.
     
     Args:
     	 clientid: Client ID of the model to use.
     	 usecase: Usecase for which detection is to be run.
     	 confidence: Confidence level of detection. Default is 45
     	 nms: Number of million pixels to use for iou detection. Default is 25
     	 image: Image to be used for detection. Must be a file object.
     
     Returns: 
     	Dictionary with the following keys : image_size (widht, height), count_detection (total detections), result (list of boundig boxes coordinates) 
    """
    
    
    # Use CustomEmissionsTracker to send emmisions to databases
    with CustomEmissionsTracker(DB_CONNECTION_STRING, TABLE_NAME):
        model = apihelper.get_model(clientid, usecase)
        # If model is None raise HTTPException status_code 400 detail 400
        if model is None:
            raise HTTPException(status_code=400, detail="Model not exists for this usecase")
        # Model configuration
        model.conf = float(confidence)
        model.iou = float(nms)
        model.max_det = 10000
        model.agnostic = True
        # Convert image input to byte format
        input_image = apihelper.get_image_from_bytes(image)
        # Make prediction using the model
        results = model(input_image, size=1280)
        # Get prediction list
        detect_res = results.pandas().xyxy[0].to_json(orient="records")
        detect_res = json.loads(detect_res)
        # Make JSON output
        data_result = {
            "image_size": {
                "width": results.ims[0].shape[1],
                "height": results.ims[0].shape[0]
            },
            "count_detection": apihelper.count_prediction(results),
            "result": detect_res,
        }
        
        return data_result

@app.post("/predictImagePath")
async def predict_image_path(clientid: str, usecase: str, confidence: float = 0.45, nms: float = 0.25, image: UploadFile = File(...)):
    """
     Run object detection and return image with bounding boxes as a base64 encoded string
     
     Args:
     	 clientid: Client ID to connect to model
     	 usecase: Usecase to use for model. Should be unique for each client
     	 confidence: Confidence level of model.
     	 nms: Number of millimeters of object detection
     	 image: Image to be used for prediction
     
     Returns: 
     	 Path to predicted image with bounding boxes as a base64
    """
    
    
    # Use CustomEmissionsTracker to send emmisions to databases
    with CustomEmissionsTracker(DB_CONNECTION_STRING, TABLE_NAME):
        model = apihelper.get_model(clientid, usecase)
        # If model is None raise HTTPException status_code 400 detail 400
        if model is None:
            raise HTTPException(status_code=400, detail="Model not exists for this usecase")
        
        model.conf = float(confidence)
        model.iou = float(nms)
        model.max_det = 10000
        
        input_image = Image.open(image.file)
        img_np = np.array(input_image)
        
        results = model(img_np, size=1280)
        
        line_thickness = 1
        # Draw a bouding boxes of the image with the given xyxy.
        for *xyxy, conf, cls in results.xyxy[0]:
            x1, y1, x2, y2 = map(int, xyxy)
            cv2.rectangle(img_np, (x1, y1), (x2, y2), color=(255, 0, 0), thickness=line_thickness)
        
        img_np = np.rot90(img_np, 2)
        img_final = Image.fromarray(img_np)
        bytes_io = io.BytesIO()
        img_final.save(bytes_io, format="JPEG")
        bytes_io.seek(0)
        
        bucket_name = 'microteks'
        folder_name = 'images-results'
        image_path = f"{folder_name}/{image.filename}"
        image_url = apihelper.upload_file_to_space(bytes_io, bucket_name, object_name=image_path)
    
    return JSONResponse(content={"image_url": image_url})

@app.post("/predictImageOnly")
async def predict_image_only(clientid: str, usecase: str, confidence: float = 0.45, nms: float = 0.25, image: bytes = File(...)):
    """
     Predict an image using a model. This is a low - level function for use in testing and will return a list of predictions in the form of ( x y confidence class )
     
     Args:
     	 clientid: Client ID of the model to use
     	 usecase: Usecase of the model to use e. g.
     	 confidence: Confidence level of the model. Default is 45
     	 nms: Interest level of the model. Default is 25
     	 image: Image to use as input. Default is File ()
     
     Returns: 
     	 Predicted image object with bouding doxes
    """
    
    # Use CustomEmissionsTracker to send emmisions to databases
    model = apihelper.get_model(clientid, usecase)
    # If model is None raise HTTPException status_code 400 detail 400
    if model is None:
        raise HTTPException(status_code=400, detail="Model not exists for this usecase")
    
    model.conf = confidence
    model.iou = nms
    model.max_det = 10000
    
    input_image = Image.open(io.BytesIO(image))
    img_np = np.array(input_image)
    
    results = model(img_np, size=1280)
    
    line_thickness = 1
    # Draw bouding boxes of the image with the given xyxy.
    for *xyxy, conf, cls in results.xyxy[0]:
        x1, y1, x2, y2 = map(int, xyxy)
        cv2.rectangle(img_np, (x1, y1), (x2, y2), color=(255, 0, 0), thickness=line_thickness)
    
    img_np = np.rot90(img_np, 2)
    img_final = Image.fromarray(img_np)
    bytes_io = io.BytesIO()
    img_final.save(bytes_io, format="JPEG")

    return Response(content=bytes_io.getvalue(), media_type="image/jpeg")

@app.get("/syncmodel")
async def syncmodel(modelName: str):
    """
     Synchronize the model after copying it to the remote DigitalOcean space
     
     Args:
     	 modelName: Name of the model to sync
     
     Returns: 
     	 Success Message or error
    """
    
    endpoint_url = "https://fra1.digitaloceanspaces.com"
    space_name = "microteks"
    do_folder = "model-deploy"
    local_folder = "model"
    
    s3 = boto3.client(
        "s3",
        region_name='fra1',
        endpoint_url=endpoint_url,
        aws_access_key_id='access_key',
        aws_secret_access_key='secret_access'
    )
    
    try:
        response = s3.list_objects(Bucket=space_name, Prefix=do_folder)
        objects = response.get("Contents", [])
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to list DigitalOcean folder")

    # return 404 if DigitalOcean folder is empty
    if not objects:
        raise HTTPException(status_code=404, detail="DigitalOcean folder is empty")

    # downloads the file and updates the model
    for obj in objects:
        key = obj["Key"]
        # downloads a file from the local folder and updates the model
        if key.endswith(".pt") and os.path.basename(key).split('.')[0] == modelName:
            print('modelName found =', modelName)
            local_file_path = f'{local_folder}/{os.path.basename(key)}'
            do_file_path = f'{do_folder}/{os.path.basename(key)}'
            print('local_file_path =', local_file_path)
            print('do_file_path =', do_file_path)
            s3.download_file(space_name, do_file_path, local_file_path)
            apihelper.update_model(modelName)

    return {"message": "Model synchronization completed successfully"}

@app.post("/addmodel")
async def addmodel(client: str, usecase: str, modelName: str):
    """
     Add a model entry to the configuration file. This will overwrite an existing entry with the same name
     
     Args:
     	 client: The client .
     	 usecase: The usecase .
     	 modelName: The name of the model. It is used to generate the name for the model and update the entry in the configuration file.
     
     Returns: 
     	 Success Message 
    """
    
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)

    new_entry = {
        "name": f"{client}_{usecase}",
        "model": modelName
    }

    # update the entry in the config
    for entry in config:
        # update entry model if the new entry is the same as the old one
        if entry['name'] == new_entry['name']:
            print('update entry =', new_entry)
            entry['model'] = new_entry['model']
            break
    else:
        print('add entry =', new_entry)
        config.append(new_entry)
    
    with open('config.json', 'w') as config_file:
        json.dump(config, config_file, indent=4)
    
    apihelper.add_model_entry_to_list(client, usecase, modelName)
    
    return {"message": "Model added/updated in the config successfully."}

@app.get("/emissions/")
async def get_emissions():
    """
     Get emissions by source and date. This endpoint is used to query database for emissions based on source and date.
     
    """
   
    result = apihelper.get_emissions_by_source_and_date()
    # If the database connection failed raises an HTTPException.
    if result is None:
        raise HTTPException(status_code=500, detail="Database connection failed")
    # HTTPException HTTPException status_code 404 detail emission data not found
    if not result:
        raise HTTPException(status_code=404, detail="Emission data not found")
    return result