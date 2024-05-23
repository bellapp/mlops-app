from fastapi import HTTPException
import torch
from PIL import Image
import io
from fastapi.encoders import jsonable_encoder
import configparser
import boto3
import os
import json
import  mysql.connector
import pathlib
from botocore.exceptions import NoCredentialsError

print("os name = " , os.name)
# Checki if the operating system is Windows (`'nt'` stands for Windows NT).
# If the condition is true, it temporarily stores the original `pathlib.PosixPath` class in the
# variable `temp`, and then replaces `pathlib.PosixPath` with `pathlib.WindowsPath`.
if os.name == 'nt':
    temp = pathlib.PosixPath
    pathlib.PosixPath = pathlib.WindowsPath


# List of usecase name an their models
nameList=[]
modelList=[]
# Dictionary to hold the models
modelDict = {}
# Database configuration
base_db_config = {
    'host':'ip_address',
    'user':'use',
    'password':'enrypted_password',
    'database':'dn_name'
}
# S3 Digitalocean configuration
s3 = boto3.client(
            "s3",
            region_name='fra1',
            endpoint_url='https://fra1.digitaloceanspaces.com',
            aws_access_key_id='ACCESS_ID',
            aws_secret_access_key='ACCESS_KEY'
        )
# Main bucket name
space_name = "microteks"

# Function to create a database connection with a dynamic database name
def get_db_connection(database_name):
    """
     Get a connection to the database. This is a helper function to make it easier to use the DB API
     
     Args:
     	 database_name: The name of the database
     
     Returns: 
     	 A connection to the database or None if there was an
    """
    db_config = base_db_config.copy()
    db_config['database'] = database_name  # Set the database name
    try:
        return mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        print(f"Database Error: {err}")
        return None


    
def downloadmodels(is_download_file: True):
    """
     Downloads model files from DigitalOcean Spaces. If downloadmodels is set to True the function will list objects in DigitalOcean Spaces and the local folder to save the models.
     
     Args:
     	 is_download_file: determines whether to download model
    """
    
    # Define the folder in DigitalOcean Spaces and the local folder to save the models
    do_folder = "model-deploy"
    local_folder = "model"
    objects = None

    # Download the file from DigitalOcean Spaces to the local folder
    if is_download_file:
        try:
            # List objects in the specified DigitalOcean Spaces folder
            response = s3.list_objects(Bucket=space_name, Prefix=do_folder)
            objects = response.get("Contents", [])
        except Exception as e:
            # Raise an HTTPException if listing objects fails
            raise HTTPException(status_code=500, detail="Failed to list DigitalOcean folder")

        # Returns a 404 if the folder is empty
        if not objects:
            # Raise an HTTPException if the folder is empty
            raise HTTPException(status_code=404, detail="DigitalOcean folder is empty")

        # Load the objects from DigitalOcean Spaces to the local folder
        for obj in objects:
            key = obj["Key"]
            
            # Load the file from DigitalOcean Spaces to the local folder
            if key.endswith(".pt"):  # Check if the object is a model file
                modelName = os.path.basename(key).split('.')[0]
                print(f"Load model {modelName}")
                
                # Define the local and DigitalOcean Spaces file paths
                local_file_path = f'{local_folder}/{os.path.basename(key)}'
                do_file_path = f'{do_folder}/{os.path.basename(key)}'
                
                # Download the file from DigitalOcean Spaces to the local folder
                s3.download_file(space_name, do_file_path, local_file_path)
            
    # Read the JSON config file
    with open('../config.json', 'r') as f:
        config = json.load(f)
    
    # Download models and populate the dictionary
    for item in config:
        name = item['name']
        nameList.append(name)  # Append the name to the name list
        model = item['model']
        modelList.append(model)  # Append the model to the model list
        print(f"Load {model}")
        
        # Load the model using torch.hub and save it to a dictionary
        modelTemp = torch.hub.load('ultralytics/yolov5', 'custom', path=f'./model/{model}.pt', force_reload=False, trust_repo=True)
        print(f"{model} Loaded")
        modelDict[name] = modelTemp  # Add the loaded model to the dictionary
    
    print("Model synchronization completed successfully")

# Add individual model to model list
def add_model_entry_to_list(client, usecase, modelName):
    """
     Add a model entry to the list of models. This is done by adding the client name usecase and model name to the modelDict
     
     Args:
     	 client: name of the client to add
     	 usecase: usecase of the client to add ( should be unique )
     	 modelName: name of the model to add ( should be unique
    """
    clientName = f"{client}_{usecase}"
    nameList.append(clientName)
    modelList.append(modelName)
    modelDict[clientName] = None
    print('modelDict after add model = ', modelDict)

# upload image to DO space
def upload_file_to_space(file_name, bucket, object_name=None):
    """
     Upload a file to an S3 bucket. This is a wrapper around the boto. s3. upload_fileobj function to allow uploading files to a bucket without having to worry about credentials.
     
     Args:
     	 file_name: The name of the file to upload
     	 bucket: The name of the bucket to upload to
     	 object_name: The name of the object to upload to
     
     Returns: 
     	 True if successful False if not 
    """
    # Set the object name to the file name.
    if object_name is None:
        object_name = file_name

    try:
        response = s3.upload_fileobj(file_name, bucket, object_name)
    except NoCredentialsError:
        print("Credentials not available")
        return False
    return f"{object_name}"


# get the model based on cient and uscase
def get_model(clientid,usecase):
    """
     Returns the model associated with a client ID and use case from a dictionary called ` modelDict `
     
     Args:
     	 clientid: The unique identifier for a client
     	 usecase: The usecase for which the model is being used
     
     Returns: 
     	 The model associated with the given client ID and use
    """
    clientName = f"{clientid}_{usecase}"
    
    # Get the model object for the clientName.
    if clientName in modelDict:
        model = modelDict[clientName]
    else :
        model= None
    return model


def get_image_from_bytes(binary_image, max_size=1024):
    """
     Converts a binary image to a PIL image. Resizes the image to max_size
     
     Args:
     	 binary_image: The binary image to convert
     	 max_size: The maximum size of the image in megabytes
     
     Returns: 
     	 An PIL image of the specified size and the same
    """
    input_image = Image.open(io.BytesIO(binary_image)).convert("RGB")
    width, height = input_image.size
    resize_factor = min(max_size / width, max_size / height)
    resized_image = input_image.resize(
        (
            int(input_image.width * resize_factor),
            int(input_image.height * resize_factor),
        )
    )
    return resized_image


def count_prediction(result):
    """
     Count the number of objects detected for each class. This is used to create a JSON response that can be sent to the client
     
     Args:
     	 result: The result of the prediction
     
     Returns: 
     	 A JSON response with the class counts as a dictionary
    """
    # Count the number of objects detected for each class
    class_counts = {}
    class_names = result.names
    # Counts the class names of the classes in the output.
    for output in result.pred:
        # Add a label to the class_counts.
        for label in output[:, -1]:
            class_name = class_names[int(label)]
            # Increments the class count for the given class name.
            if class_name in class_counts:
                class_counts[class_name] += 1
            else:
                class_counts[class_name] = 1

    # Return the class counts as a JSON response
    return jsonable_encoder(class_counts)

def update_model(modelName):
    """
     Update the model dictionary to reflect the model that was loaded. This is a function for use in testing
     
     Args:
     	 modelName: name of the model
    """
    # get the index of modelName
    modelIndex = modelList.index(modelName)
    # Affect clientName(client_usecase) the model
    clientName = nameList[modelIndex]
    modelTemp = torch.hub.load('ultralytics/yolov5', 'custom', path=f'./model/{modelName}.pt', force_reload=False)
    modelDict[clientName]=modelTemp




# Function to get emissions by source and date
def get_emissions_by_source_and_date():
    """
     Get emissions by source and date. This is useful for finding a source's emissions in a given time frame.
     
     Returns: 
     	 a list of tuples ( source date emission ) grouped by Source (MODEL_SERVINE, ACTIVE_LEARNING)
    """
    cnx = get_db_connection('codecarbondb')
    # Return the current connection to the database.
    if cnx is None:
        return None

    cursor = cnx.cursor(dictionary=True)
    query = """
        SELECT source, DATE(timestamp) as date, SUM(emission) as total_emission
        FROM emission
        GROUP BY source, DATE(timestamp)
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    cnx.close()
    return result

