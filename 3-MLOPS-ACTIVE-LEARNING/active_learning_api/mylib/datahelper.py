import boto3
from botocore.client import Config
import os
import tempfile
import posixpath
import glob
import shutil
import mlflow
from mlflow.tracking.client import MlflowClient
import requests

from sqlalchemy import create_engine, cast, Integer,union
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import or_, text
from sqlalchemy.sql import func
from sqlalchemy.exc import SQLAlchemyError
# Initialize a session using DigitalOcean Spaces.

os.environ['AWS_ACCESS_KEY_ID'] = 'ACCESS_ID'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'SECRET_ACCESS'
os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'https://fra1.digitaloceanspaces.com'

# Initialize boto3 session
session = boto3.session.Session()

# Use the session to create both client and resource
s3_client = session.client('s3', endpoint_url='https://fra1.digitaloceanspaces.com')
s3_resource = session.resource('s3', endpoint_url='https://fra1.digitaloceanspaces.com')
ROOT_BUCKET= 'microteks'


# List all buckets on your account.
response = s3_client.list_buckets()
# print(response['Buckets'])
spaces = [space['Name'] for space in response['Buckets']]
# print("Spaces List: %s" % spaces)
response = s3_client.list_objects(Bucket=ROOT_BUCKET)

#init Database param
host ="address"
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://ip_address/dbname"

engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# mlflow
# mlflow client
mlflow_tracking_uri = "http://active-learning.microteks.io:5000"
mlflow.set_tracking_uri(mlflow_tracking_uri)
mlflow_client = MlflowClient()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
Base = automap_base()
Base.prepare(engine, reflect=True)

# for obj in response['Contents']:
# # for obj in response['Contents'].objects.filter(Prefix="data-images-al/LEMPA_LAB//"):
#     print(obj['Key'])

# s3 = boto3.resource('s3')
# bucket = s3.Bucket('my-bucket')
# objs = bucket.objects.filter(Prefix='myprefix')
# for obj in objs:
#     pass



async def copy_file_to_folder(source_file,destination_file_path, source_bucket = ROOT_BUCKET,destination_bucket= ROOT_BUCKET):
    source_bucket = ROOT_BUCKET
    destination_bucket = ROOT_BUCKET
    print('copy_file_to_folder -> source= ',source_file)
    print('copy_file_to_folder ->dest= ',destination_file_path)
    s3_client.copy_object(
               CopySource={'Bucket': source_bucket, 'Key': source_file},
               Bucket=destination_bucket,
               Key=destination_file_path)
    
    return True
def replace_class_by_order(file_name,list_classes):
    print('#### replace_class_by_order')
    # Download the file from Spaces
    labelfile = os.path.basename(file_name)
    temp_dir = tempfile.mkdtemp()
    local_file = os.path.join(temp_dir,labelfile)
    print('==== replace_class_by_order file_name = ', file_name)
    # print('labelfile = ',labelfile)
    print('=== replace_class_by_order local_file = ', local_file)
    # print('before download')
    # 'd:\\test.txt'
    s3_client.download_file(ROOT_BUCKET, file_name, local_file)
    print('after download')
    # Read the file
    with open(local_file, 'r') as file:
        lines = file.readlines()
        # lines = file.read()
    # print('#### lines')
    # print('lines = ',lines)
    for i,line in enumerate(lines):
        # print('line 1 = ',line)
        values = line.strip().split()
        # print('values[0] 1= ',int(values[0]))
        if int(values[0]) < 10:
            # print('dsqdqsd')
            return
        # print('list_classes = ', list_classes)
        order_class = str(list_classes[int(values[0])]['order_class'])
        # print('order_class = ', order_class)
        values[0] = order_class  # Replace the first word with '0'
        # print('values = ', values)
        lines[i] = ' '.join(values) + '\n'
        # print(' values = ',values )
    # print('lines   = ', lines)
    with open(local_file, 'w') as file:
        file.writelines(lines)

    # Upload the updated file back to Spaces
    s3_client.upload_file(local_file, ROOT_BUCKET, file_name)

def create_folder( directory_name):
    bucket_name=ROOT_BUCKET
    s3_client.put_object(Bucket=bucket_name, Key=(directory_name + '/'))

async def copy_folder(source_folder,destination_folder, source_bucket = ROOT_BUCKET,destination_bucket= ROOT_BUCKET):
    print('### copy folder')
    print("source_folder = ", source_folder)
    print("destination_folder = ", destination_folder)
    print("source_bucket = ", source_bucket)
    print("destination_bucket = ", destination_bucket)
    response = s3_client.list_objects_v2(
    Bucket=source_bucket,
    Prefix=source_folder)
    print(source_bucket, source_folder)

    for content in response.get('Contents', []):
        source_key = content['Key']
        print('source',source_key)
        destination_key = source_key.replace(source_folder, destination_folder)
        print('dest',destination_key)
        s3_client.copy_object(
        CopySource={
            'Bucket': source_bucket,
            'Key': source_key
        },
        Bucket=destination_bucket,
        Key=destination_key
    )
    print('###  folder copied successfully')
    return True

# copy list of files from s3 to local folders

def copy_files_from_spaces(file_list, local_folder):
    s3_dir_image = 'data-images-al/CONIDIA/FUNGI_BACTERIA/feedback/images'
    s3_dir_label = 'data-images-al/CONIDIA/FUNGI_BACTERIA/feedback/labels'
    print(s3_dir_image)
    # Iterate through the file list and copy each file to the local folder
    for file_key  in file_list:
        # Specify the source file location in the DigitalOcean Space
        # source_file = {
        #     'Bucket': ROOT_BUCKET,
        #     'Key': os.path.join(s3_dir,file_key+'.jpg')
        # }

        image_source_file = posixpath.join(s3_dir_image,file_key+'.jpg')
        label_source_file = posixpath.join(s3_dir_label,file_key+'.txt')
        print(image_source_file)
        print(label_source_file)
        # Specify the destination file location on the local machine
        image_destination_file = f'{local_folder}\\{os.path.basename(image_source_file)}'
        label_destination_file = f'{local_folder}\\{os.path.basename(label_source_file)}'
        print(image_destination_file)
        print(label_destination_file)

        try:
            # Check if the file exists in DigitalOcean Spaces
            s3_client.head_object(Bucket=ROOT_BUCKET, Key=image_source_file)
            # Copy the file from DigitalOcean Spaces to the local folder
            s3_client.download_file(ROOT_BUCKET, image_source_file, image_destination_file)
            s3_client.download_file(ROOT_BUCKET, label_source_file, label_destination_file)
        except :
            print(f'{file_key} not found')

        print(f'Copied file: {image_source_file} to {image_destination_file}')
def verify_files_exists(file_list, local_folder):
    s3_dir_image_train = 'data-images-al/CONIDIA/FUNGI_BACTERIA/raw-data/images/train'
    s3_dir_image_val = 'data-images-al/CONIDIA/FUNGI_BACTERIA/raw-data/images/val'
    # s3_dir_label = 'data-images-al/CONIDIA/FUNGI_BACTERIA/raw-data/labels/train'
    # print(s3_dir_image)
    # Iterate through the file list and copy each file to the local folder
    for file_key  in file_list:
        image_source_file_train = posixpath.join(s3_dir_image_train,file_key+'.jpg')
        image_source_file_val = posixpath.join(s3_dir_image_val,file_key+'.jpg')
        # label_source_file = posixpath.join(s3_dir_label,file_key+'.txt')
        # print(image_source_file_train)
        # print(image_source_file_val)
        try:
            # Check if the file exists in DigitalOcean Spaces
            s3_client.head_object(Bucket=ROOT_BUCKET, Key=image_source_file_train)
        except :
            try:
                s3_client.head_object(Bucket=ROOT_BUCKET, Key=image_source_file_val)
            except:
                print(f'{file_key} not found')



def get_model_name(db: Session, client: str, usecase: str):
    """
    Retrieves the model name for a given client and usecase from the database.

    Args:
        db: The database session.
        client: The client id.
        usecase: The usecase id.

    Returns:
        The model name of the usecase, or an error message if not found.
    """
    try:
        Usecase = Base.classes.usecases  # Ensure Base is properly imported
        usecase_model_name = db.query(Usecase.model_name).filter(
            Usecase.client == client,
            Usecase.usecase == usecase,
        ).one()[0]
        return usecase_model_name
    except Exception as e:
        return {"message": str(e)}
    
async def update_model_name(db: Session, client: str, usecase: str, model_name: str):
    """
    Updates the model name in the usecase table for a given client and usecase.

    Args:
        db: The database session.
        client: The client identifier.
        usecase: The usecase identifier.
        model_name: The new model name to be updated.

    Returns:
        A success message if the update is successful, or an error message if not.
    """
    print(f"update model name for client '{client}' and usecase '{usecase}'")
    try:
        Usecase = Base.classes.usecases  # Ensure Base is properly imported
        # Find the usecase entry to update
        usecase_entry = db.query(Usecase).filter(
            Usecase.client == client,
            Usecase.usecase == usecase
        ).one()
        print('usecase_entry = ', usecase_entry)
        # Update the model name
        usecase_entry.model_name = model_name
        db.commit()

        return f"Model name updated to '{model_name}' for client '{client}' and usecase '{usecase}'"
    except Exception as e:
        print('Error', e)
        db.rollback()
        return {"message": str(e)}
def copy_images_db(db: Session, src_client: str, src_usecase: str, dest_client: str, dest_usecase: str):
    """
    Copies all image records from one (client, usecase) to another.

    Args:
        db: The database session.
        src_client: The source client id.
        src_usecase: The source usecase id.
        dest_client: The destination client id.
        dest_usecase: The destination usecase id.

    Returns:
        A success message with the number of copied images or an error message.
    """
    try:
        Images = Base.classes.images  # Ensure Base is properly imported
        src_images = db.query(Images).filter(
            Images.client == src_client,
            Images.usecase == src_usecase,
            Images.is_feedback == 0,
        ).all()

        for img in src_images:
            new_image = Images(
                client=dest_client,
                usecase=dest_usecase,
                image_name=img.image_name,
                path=img.path,
                version=img.version,
                labelfile=img.labelfile,
                class_count=img.class_count,
                date_time=img.date_time,
                is_trained=img.is_trained,
                is_feedback=img.is_feedback,
                app_batch_id=img.app_batch_id
            )
            db.add(new_image)

        db.commit()

        return {"message": f"Successfully copied {len(src_images)} images."}

    except SQLAlchemyError as e:
        db.rollback()
        return {"error": str(e)}
    

async def add_model(client: str, usecase: str, modelName: str):
    print(" addmodel method")
    URL_ROOT = "http://api.microteks.io/"
    url= URL_ROOT + "addmodel"
    
    params = {
        "client": client,
        "usecase" : usecase, 
        "modelName": modelName,
    
    }


    response = requests.post(url,params=params)
    # Check the response
    model_name=""
    print(" response addmodel = ",response.status_code)
    if response.status_code == 200 :
        print(" addmodel method response = ", response.json())
        model_name = response.json()
        if len(model_name)>0:
            print("addmodel method response success")
    # Request was successful
            model_name = response.json()
    return model_name

async def sync_model(modelName):
    URL_ROOT = "http://api.microteks.io/"
    url= URL_ROOT + "syncmodel"
    
    params = {
        
    "modelName": modelName,
    
    }
    response = requests.get(url,params)
    # Check the response
    model_name=""
    if response.status_code == 200 and len(model_name)>0:
    # Request was successful
        model_name = response.json()
    return model_name

def remove_lines_starting_with_1(dir_path: str):
    # iterate over all .txt files in the directory
    for file_path in glob.glob(os.path.join(dir_path, '*.txt')):
        with open(file_path, 'r') as f:
            lines = f.readlines()

        # create a temporary file and open it for writing
        with tempfile.NamedTemporaryFile(mode='w+t', delete=False) as tf:
            for line in lines:
                # write line to temporary file if it doesn't start with "1"
                if not line.startswith('1'):
                    tf.write(line)

            tempname = tf.name

        # replace the original file with the temporary file
        shutil.move(tempname, file_path)



# Copy mlflow run from source experiment to target experiment
# Function to download directory from S3
def downloadDirectoryFroms3(bucket_name, remote_directory_name, local_directory):
    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=remote_directory_name):
        print('obj = ', obj)
        if not obj.key.endswith('/'):  # Skip directories
            local_file_path = os.path.join(local_directory, os.path.relpath(obj.key, remote_directory_name))
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            s3_client.download_file(bucket_name, obj.key, local_file_path)

def get_production_run_id( experiment_name, model_name):
    exp_id = mlflow_client.get_experiment_by_name(experiment_name).experiment_id
    
    experiment_runs = mlflow_client.search_runs(experiment_ids=[exp_id])
    print(len(experiment_runs))
    for run in experiment_runs:
        # print(run)
        models = mlflow_client.search_model_versions(f"name='{model_name}'")
        for model in models:
            if model.current_stage == 'Production' and model.run_id == run.info.run_id:
                print(run.info.run_id)
                return run.info.run_id
    return None

async def copy_run(source_exp_name, target_exp_name, source_model_name, target_model_name):
    # mlflow.set_tracking_uri(mlflow_tracking_uri)
    # client = MlflowClient()

    # Get the source run ID from the production model in the source experiment
    source_run_id = get_production_run_id( source_exp_name, source_model_name)

    if source_run_id is None:
        print("No production model found in the source experiment.")
        return

    source_run = mlflow_client.get_run(source_run_id)
    mlflow.set_experiment(target_exp_name)

    with mlflow.start_run() as new_run:
        # Log parameters and metrics
        for key, value in source_run.data.params.items():
            mlflow.log_param(key, value)
        for key, value in source_run.data.metrics.items():
            mlflow.log_metric(key, value)

        # Copy artifacts
        artifact_uri = source_run.info.artifact_uri
        artifacts = mlflow_client.list_artifacts(source_run_id)
        if not os.path.exists('mlflow_output'):
            os.makedirs('mlflow_output')
        for artifact in artifacts:
            local_path = artifact.path
            if artifact.is_dir:
                print('local_path 1 = ',local_path)
                download_path = f"{artifact_uri}/{local_path}"
                object_key = '/'.join(download_path.split('/')[3:])
                # print('object_key 1= ',object_key)
                # downloadDirectoryFroms3('microteks', f"{artifact.path}/", local_path)

                downloadDirectoryFroms3('microteks', object_key,  f"mlflow_output/{local_path}")
                mlflow.log_artifact(f"mlflow_output/{local_path}")
            else:
                
                # object_key = f"{artifact.path}"
               
                
                download_path = f"{artifact_uri}/{local_path}"
                
                object_key = '/'.join(download_path.split('/')[3:])
                print('local_path 2= ',local_path)
                print('object_key 2= ',object_key)
                s3_client.download_file('microteks', object_key,  f"mlflow_output/{local_path}")
                mlflow.log_artifact(f"mlflow_output/{local_path}")
         # Register the model and set it to production
        model_path = f"artifacts/best.pt"
        model_uri = f"runs:/{new_run.info.run_id}/{model_path}"
        registered_model = mlflow.register_model(model_uri=model_uri, name=target_model_name)

        # Transition the model version to production
        mlflow_client.transition_model_version_stage(
            name=target_model_name,
            version=registered_model.version,
            stage='Production'
        )

    return new_run.info.run_id


# Example usage
# files_to_copy = [
# 'JhKGbi7xPXrh3pTO2IoZIdtqk72bnzGZvALTVPn8',
# 'D41r9qQpJ6VE6h2zIK1wZnDedXm58Yhu27X0SZbm',
# 'D41r9qQpJ6VE6h2zIK1wZnDedXm58Yhu27X0SZbm',
# 'paBEkmlRq78Av7uQmfAONI9we46qf7Vh0FILYBxE',
# 'Bwjx7840Q52pORiJyz6BcQxSLUntiOulFJqHrcdp',
# 'wSxfGSpxjKGyWtdIcIjmsfWslcxXeymhsPBqmIqe',
# 'CBJkHmodsDJ2s2TF2MF2lbzZB21piA1obUMBHnpm',
# 'krWdCzpn6XxyY7X5G4vUwCy8TVfiQRq9sucOiZpY',
# '9cwQVw4CjLOeWzowZboaJrSTnPgj6CTYKDX7f4by',
# 'UYa3E0Pk85yNsA653awNPMBKxkJocSo5TKfnYSKF',
# 'YhdaOLuZKUp8oskEcdx5fDl4RL9KGvsduBxoLJ5b',
# 'WlzwC4oWY6vzyaCw5Kb4sGh8VJNaRjrhRr3cMRTi',
# 'ASjtAAzZDz8hlHh1GO0VoVINzWdzi1yveWYStjcF',
# 'QCnlrnY2j4W5pQP3R9jxVG8Uv1ii73zo9YBcFTa2',
# '1wnstDAc8i4Qqq7v3tkCm72xf9WxsUnvewIET8Wl',
# 'dVcuQOXHide1p4HA5UFUpcMzPVjtflZ9mWri87OC',
# 'R5dPQH3SIpHGBN0U2m0L6de9pUfeyTO0EqqhKjCG',
# 'GBcc3TmDNM9mMPQUTgzWBiL5NeeCB81ovEteDQXV',
# 'tXRdXZ672MRKTlmGYiCfA9Stkvj1TY157Sff95Vi',
# 'CzSoKmUf1t7Vb5CpHnJE5Q2xIZ9jRh2AOG9KS3DD',
# '0bKSDjpfvsloQDjjAh74jm16hBRxjkjaWH1gOGHm',
# 'OyX7GX6UHQTRoMbmdZa8XM8SxPBoHyTM5L5Fuxe3',
# 'lRWG22NGWRSetjdHnOmOE2w25XnyfzTmjbdwpTdq',
# 'CaYSQnGvtYauMIa36WptjeYCGndWK0CDaOCThdcm',
# 'Muk8FoXDeQuetzdkNSHO2uKVH5fEHfvsG5bPbl8h',
# 'SO1nQmbyGJczI2gLFncHhuwwT6Z5N8WTa87LEYDi',
# 'D2DXrCwHYQ3zZUHNpeVRgWMhggpnQsNdjqJWI3YT',
# 'oMoDJXhjB83M0Vsr0lHYVN8SkZzTZNgeRSnlofxq',
# '1KIptLaEdMgvhahbzBcHQlhuo96GZ4D1ZOGmvNe5',
# 'dCihk7zCt916bzcCqgsc5WsHeuQQ2ONhLBTtj523',
# 'VEWBAtuPnRf6m19SHyx4NOV9G2gGTBPrfEqOJdLH',
# 'NMcwtgJ6Z6mHTLIhkBBBrry4xRt6EXfMXaRmE4Yd',
# 'GClte9FxzDOIlGranbFVES6jATg7AXxqIdK6DZ7t',
# 'MAuJYoCtDiTAYh6vIDiH8K7fdS2UUCRy2nedCCOs',
# '4E5iPU08cptINXRF9naSlcG9plhhx6s2hndNBXRN',
# '5o251o0a898lNAx6LhMbnPI9OKjblrwVLa2O4R9Q',
# 'as9bRpF2K7iywjwIc5wDFPWVXaXxa21NBdtNlmB6',
# 'kEWk6RdcHmLZRCdwbpBQ04IAi3xDbeeM84u9IuOW',
# 'yeXtKD7KRbKZVqHVxzA4yWBey43Np8lEiZLsreih',
# 'LHTFrkqk38Z9UH4UQf8NeQYgseXzXlGQcapoT2YF',
# 'fQD6paHYUyUQjsuyt5jZkoCOr7qONitvaVy0ZesE',
# 'jet8LEPlqgJZ0FZxxzWsBbhBL4kx3deAQ5Senp3q',
# 'GtCTXnZXZTixbBSCRsv4QDyMNHZVROzYtsdCn6cJ',
# 'bInn83creItrW1FPTHKfzp68viFXsArf8xLMuApD',
# '8xEJXNDz6GpJcuX2ENJ6aVw5TlPyAWvgJpTIBtgu',
# 'YDm8GZaBuK0OfnmdDM3naoqYB6iF9lHvzyeDVkjm',
# 'yCUKWPzPXrAMeShcHJQ4iO2truN5cF0qfnliCjXk',
# 'fO42XxhjaJaeyksPi89bDkVU1L4oaXoj6LMRemKY',
# 'WjtJcmnQmaf3OXVkQUasKTI0VEa27XzYWhIqy3IU',
# 'AiNXVtR0AKTEA9vwIjjyJkRTlHFmHlweN4o6GOeq',
# 'WxXDkKLsTHpwDIupA1IAYRV8SgbymC0iH5nwBCRJ',
# '148SmezJ2NLSIv4TrsvxOhyTqmINP4nbnHt2CjMJ',
# 'uUUdBqDka7mZVYuboqtBPrv8mgEdLazh8A0xEyXC',
# 'cOyh3Dq5E8Cm8AePIRRUs4Kmc3WkEAfoTIkBqbJF',
# 'fZxdEmzb3fsAsDDhqxIEXtfyu4I9KwBK5Hnht1tU',
# 'ifoiaTTXE7BX0bBJlDArQGtWYShalKXX4gcpReHq',
# 'TSb4k7emDPwEDJTqhf8jOVzSTSom90WHIYmT4RcS',
# 'AM6XPtuOATMoYSvVblXudAAAsvBNTLLLjRdqp3cU']
# local_destination = 'd:\\conidia_bacteria_2'

# copy_files_from_spaces(files_to_copy, local_destination)
# verify_files_exists(files_to_copy, local_destination)
# remove_lines_starting_with_1(local_destination)
# source_bucket = 'microteks'
# source_folder = 'sourcedir'
# destination_bucket = 'microteks'
# destination_folder = 'test2'
# copy_folder(source_bucket=source_bucket,source_folder=source_folder,destination_bucket=destination_bucket,destination_folder=destination_folder)

# response = client.list_objects_v2(
#     Bucket='microteks',
#     Prefix='sourcedir'
# )
# keys = []
# for content in response.get('Contents', []):
#     keys.append(content['Key'])
#     print(content['Key'])

# for key in keys:
#     copy_source = {
#         'Bucket': 'microteks',
#         'Key': key
#     }
#     bucket = s3_resource.Bucket('otherbucket')
#     bucket.copy(copy_source, 'otherkey')

# keys = []
# for obj in response['Contents']:
#     keys.append(obj['Key'])


# al_bucket = s3_resource.Bucket('microteks')
# objs = al_bucket.objects.filter(Prefix='data-images-al/')
# for obj in objs:
#     print(obj.key)
# for s3_object in al_bucket.objects.all():
#     # Need to split s3_object.key into path and file name, else it will give error file not found.
#     path, filename = os.path.split(s3_object.key)
#     al_bucket.download_file(s3_object.key, filename)
# for key in al_bucket.list(prefix='data-images-al'): 
#     print(key)
# for object_summary in al_bucket.objects.filter(Prefix="data-images-al/LEMPA_LAB/"):
#     print(object_summary.key)

# keys = []
# for obj in response['Contents']:
#     keys.append(obj['Key'])
# s3_resource = boto3.resource('s3')
# for key in keys:
#     copy_source = {
#         'Bucket': 'microteks',
#         'Key': key
#     }
#     bucket = s3_resource.Bucket('otherbucket')
#     bucket.copy(copy_source, 'otherkey')


