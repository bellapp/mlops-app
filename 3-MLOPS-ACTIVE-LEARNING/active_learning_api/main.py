from fastapi import FastAPI, HTTPException, Query, Request,Depends
from datetime import datetime
from data_api.mylib import datahelper 
import json
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict
import sys
import os
from pydantic import BaseModel
from pydantic_settings import BaseSettings
import mysql.connector
from typing import List
import mysql.connector.pooling
from sqlalchemy import create_engine, cast, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import or_, text, Integer, func
from sqlalchemy.sql import func
from typing import Optional



# Init environement setting
# The class `Settings` defines configuration settings for an API with default values for `app_name` and `env_param`.
class Settings(BaseSettings):
    app_name: str = "Active learning API"
    env_param: str = ""
    
    
# creating an instance of the `Settings` class 
settings = Settings()



# creating a FastAPI instance with the title "Active Learning API"
app = FastAPI(
    title="Active Learning API",
    description="""Get Data API""",
    version="1.0.0",
)

# configuree (CORS) middleware using the `origins` list to specify the allowed origins for CORS requests
origins = [
    "http://localhost",
    "http://localhost:8000",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# @app.get('/notify/v1/health')
@app.get('/')
def get_health():
    '''The `get_health` function returns a dictionary containing a message with information about the
    Python version and environment parameters.
    
    Returns
    -------
        A dictionary is being returned with a message key containing a formatted string that includes the
    Python version and environment parameter from the settings.
    
    '''
    
    
    return dict(msg=f'Get Data API  , {sys.version}, {settings.env_param} Environment' )


currenttime = datetime.now()
# Define the database connection 
if settings.env_param == "development":
    # Set parameters for development mode
    host =""
else:
    # Set parameters for production mode
    host =""

# Database URL
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://ip_addree/db_name"
#  Create engine for sqlalchemy datavase
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Dependency
def get_db():
    """
    The function `get_db` creates a database session and yields it for use, ensuring the session is
    closed properly afterwards.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Use the `automap_base()` function from the SQLAlchemy  to reflect the database schema and generate Python classes and objects based on
# the existing database tables. 
Base = automap_base()
Base.prepare(engine, reflect=True)

# Define the model for the data to be inserted
class ImageData(BaseModel):
    image_name :str
    path:str
    version:str 
    labelfile :str
    client:str 
    usecase:str
    class_count:list[Dict[str, int]]
    date_time: datetime = datetime.now()
    to_copy:bool = True
    app_batch_id: Optional[str] = None  # New optional field
@app.post("/addimage")
async def addimage(request: Request, db: Session = Depends(get_db)):
    '''The `addimage` function in Python adds an image to a database, handling file copying and updating
    existing records if necessary.
    
    Parameters
    ----------
    request : Request
        The `request` parameter in the `addimage` function represents the HTTP request that contains the
    data needed to add an image to the database. It is used to extract the JSON data from the request
    body and process it to add the image to the database.
    db : Session
        The `db` parameter in the `addimage` function is an instance of a database session. It is used to
    interact with the database to add or update image records. The function uses this database session
    to query existing images, update records if they already exist, or create new image records if they
    
    Returns
    -------
        The function `addimage` returns a JSON object that contains a message and either the updated image
    record if it already exists in the database and was successfully updated, or the newly added image
    record if it did not exist in the database.
    
    '''
    
    
    
    # Get the raw JSON data
    json_data = await extract_json(request)
    print('image Json Data = ',json_data )
        
    try:
        image_data = ImageData(**json_data)
        print('image_data = ' , image_data)
        
        # Copy the image data to the data folder
        if image_data.to_copy :
            # Copy the image  file to the data folder
            imagefile = os.path.basename(image_data.path)
            
            destination_image_path = 'data-images-al/'+image_data.client+'/'+image_data.usecase+'/'+'feedback/images/'+imagefile
                        
            await datahelper.copy_file_to_folder(source_file=image_data.path, destination_file_path=destination_image_path)
            # Copy the label file to the data folder
            unix_label_path = image_data.labelfile.replace('\\', '/')
            labelfile = os.path.basename(unix_label_path)
            destination_label_path = 'data-images-al/'+image_data.client+'/'+image_data.usecase+'/'+'feedback/labels/'+labelfile
            
            await datahelper.copy_file_to_folder(source_file=unix_label_path, destination_file_path=destination_label_path)
            
            result = getUsecaseClasses(image_data.client,image_data.usecase, db)
            datahelper.replace_class_by_order(file_name= destination_label_path,  list_classes = result)
        # Check if the image already exists in the database
        existing_image = db.query(Base.classes.images).filter(
            Base.classes.images.client == image_data.client,
            Base.classes.images.usecase == image_data.usecase,
            Base.classes.images.image_name == image_data.image_name
        ).first()

        if existing_image:
            # Update the existing record
            for key, value in json_data.items():
                setattr(existing_image, key, value)
            existing_image.date_time = datetime.now()  # Update the date_time
            db.commit()
            db.refresh(existing_image)
             # Log the successful API call
            log_api_call(db, "addimage", json_data, "Success") 
            return {"message": "Image record updated successfully", "image": existing_image}
        else :
            image_data.date_time = datetime.now()
            db_image =  Base.classes.images(**image_data.dict(exclude={"to_copy"}))
            db.add(db_image)
            db.commit()
            db.refresh(db_image)
            # Log the successful API call
            log_api_call(db, "addimage", json_data, "Success")
            return db_image
        
            # return {"message": "Data inserted successfully"}
    except Exception as err:
        
        print('Error = ', err)
        # Log the successful API call
        log_api_call(db, "addimage", json_data, "Failure")
        raise HTTPException(status_code=400, detail=str(err))
        # return {"Error message": err}


def log_api_call(db: Session, method: str, request_data: dict, status: str):
    '''The function `log_api_call` logs API calls to a database table with details like method, request
    data, status, and call date.
    
    Parameters
    ----------
    db : Session
        The `db` parameter is the database session that will be used to interact with the database. It
    allows the function to add a new record to the `api_history` table and commit the changes to the
    database.
    method : str
        The `method` parameter in the `log_api_call` function represents the API method being called. It is
    a string that describes the specific API method that is being invoked during the API call. This
    could be a GET, POST, PUT, DELETE, or any other HTTP method used in the API
    request_data : dict
        The `request_data` parameter in the `log_api_call` function represents the data that was received
    by the API when the call was made. This data is typically in the form of a dictionary containing
    information relevant to the API request. In the function, this data is converted to a JSON string
    using
    status : str
        The `status` parameter in the `log_api_call` function represents the final status of the API call.
    It can have two possible values: "Success" or "Failure". This parameter is used to log whether the
    API call was successful or encountered an error during its execution.
    
    '''
    
    APIHistory = Base.classes.api_history
    api_history_record = APIHistory(
        method=method,
        param=json.dumps(request_data),  # Convert the request data to a JSON string
        date_call=datetime.now(),
        status_result=status
    )
    db.add(api_history_record)
    db.commit()




def getUsecaseClasses(client,usecase, db):
    """
     This function retrieves usercase classes for a specific usecase for a specific client
     
     Args:
     	 client: id of the client for which you want to retrieve classes
     	 usecase: id of the usecase for which you want to retrieve classes
     	 db: connection to the database which is typically an instance of the Psycopg2 module which is used to interact with a PostgreSQL database
     
     Returns: 
     	 A dictionary is being returned where keys are the id_class values and values are dictionaries containing the class_name for each usercase class related to the
    """
    UsecaseClass = Base.classes.usecase_class
    print('after get UsecaseClass = ',UsecaseClass)
    rows = (
            db.query(UsecaseClass.id_class, UsecaseClass.order_class, UsecaseClass.class_name)
            .filter(UsecaseClass.client == client, UsecaseClass.usecase == usecase)
            .order_by(UsecaseClass.order_class.asc())
            .all()
            )
    print('rows = ', rows)
    result = {row.id_class: {"id_class": row.id_class, "order_class": row.order_class, "class_name": row.class_name} for row in rows}
    print('result list usercase classe = ', result)
    return result

async def extract_json(request):
    """
     Extracts JSON data from a request. This is a wrapper around : func : ` json. loads ` to allow us to use it without having to re - parse the JSON.
     
     Args:
     	 request: The request to extract data from. Must be a JSON object.
     
     Returns: 
     	 The JSON data as a dictionary or ` None ` if there is no data
    """
    json_f = await request.json()
    try:
        json_data = json.loads(json_f)
    except TypeError:
        # `data` is not a valid JSON string
        json_data = json_f 
    print('json_data = ', json_data)
    return json_data


# Add a new class/category to the database
# Define the model for the data to be inserted
class ClassData(BaseModel):
    id_class: int
    class_name: str
@app.post("/addclass")

async def addclass(request: Request, db: Session = Depends(get_db)):
    """
     Add a class to the database. This will take a JSON object as input and convert it into a ClassData object
     
     Args:
     	 request: The request that contains the class data
     	 db: The database to add the class to. If you don't specify this it will use Depends. get_db
     
     Returns: 
     	 A success message or
    """
    json_data = await extract_json(request)
    class_data = ClassData(**json_data)
    try:
        
        db_class =  Base.classes.classes(**class_data.dict())
        db.add(db_class)
        db.commit()
        db.refresh(db_class)
        return db_class

    except Exception as err:
        # print_connection_detail(cursor)
        print("Erreur = ",str(err))
        return {"Error message": str(err)}
    
@app.get("/getallclasses")
# async def addimage(image_name :str, path:str, version:str , labelfile :str, client:str , usecase:str, class_count:str, date_time: datetime = currenttime):
async def getAllClasses( db: Session = Depends(get_db)):
    """
    Get all classes from sqlalchemy. This is a wrapper for db. query
    
    Args:
        db: db connection to use.
    
    Returns: 
        list of classes in db. query ( Class )
    """
    try:
        print('get classes sqlalchemy')
        
        Class = Base.classes.classes
        db_classes = db.query(Class).all()
        # HTTPException status_code 404 detail db_classes not found
        if db_classes is None:
            raise HTTPException(status_code=404, detail="db_classes not found")
        return db_classes
    except Exception as err:
        print('host = ' , host)
        # print('mydb = ', mydb.get_server_info)
        return {"Error message": err}
    
@app.get("/getimages")
async def getImagesByClientAndUsecase(client:str , usecase:str, isfeedback: List[int] = Query([1,0]), db: Session = Depends(get_db) ):
    """
     Get images by client usecase and feedback status. This is used to get images that are associated with a particular user and have feedback status
     
     Args:
     	 client: Client id of the user
     	 usecase: Usecase id of the user ( case_id )
     	 isfeedback: List of is_feedback ids of the user
     	 db: Database connection to use for database queries. If not provided database connection will be created
     
     Returns: 
     	 a list of images
    """
    try:
        Image = Base.classes.images
        images = db.query(Image).filter(
            Image.client == client, 
            Image.usecase == usecase, 
            or_(*[Image.is_feedback == feedback for feedback in isfeedback])
        ).all()
        return images
    except Exception as e:
        return {"message": str(e)}

@app.get("/getcountfeedbackimages")
async def getcountfeedbackimages(client:str , usecase:str, isfeedback: List[int] = Query([1]) ,db: Session = Depends(get_db)
):
    """
    Count the number of images that have feedback. This is useful for determining how many images a user has uploaded to an image in order to display the feedback image.
    
    Args:
        client: The client to check for images. Can be a username or email address.
        usecase: The usecase to check for images.
        isfeedback: A list of is_feedback values to check for images.
        db: The database connection to use. Defaults to get_db.
    
    Returns: 
        The number of images or an error message if something goes wrong
    """
    try:
        Image = Base.classes.images
        count = db.query(Image).filter(
            Image.client == client, 
            Image.usecase == usecase, 
            or_(*[Image.is_feedback == feedback for feedback in isfeedback])
        ).count()
        return count
    except Exception as e:
        return {"message": str(e)}
# Add a new client/usecase to the database
# Define the model for the data to be inserted
class UsecaseData(BaseModel):
    client: str
    usecase: str
    order_usecase: int = 1
    model_version: str = '1'
    model_date: datetime = datetime.now()
@app.post("/addusecase")

# async def addusecase(request: Request):
async def addusecase(request: Request, db: Session = Depends(get_db)):
    """
     Add a usecase to the database. This will take a JSON object as input and convert it into a : class : ` UsecaseData ` object for the data to be added
     
     Args:
     	 request: The request that contains the data
     	 db: The database to add the usecase
    """
    json_data = await extract_json(request)
    usecase_data = UsecaseData(**json_data)
    try:
        Usecase = Base.classes.usecases
        next_order = db.query(func.max(Usecase.order_usecase).label("max_order")).one().max_order
        next_order = (next_order or 0) + 1  # if max_order is None (when table is empty), use 0 as default value
        usecase_data.order_usecase = next_order
        print('usecase_data = ',usecase_data)
        db_usecase =  Base.classes.usecases(**usecase_data.dict())
        db.add(db_usecase)
        db.commit()
        db.refresh(db_usecase)
       
    
        datahelper.create_folder(f'data-images-al/{usecase_data.client}/{usecase_data.usecase}/raw-data')
        datahelper.create_folder(f'data-images-al/{usecase_data.client}/{usecase_data.usecase}/feedback/images')
        datahelper.create_folder(f'data-images-al/{usecase_data.client}/{usecase_data.usecase}/feedback/labels')
        # Return a success message
        return {"message": "Usecase inserted successfully"}
    
    except Exception as err:
        
        return {"Error message": err}

# Update usecase model
@app.put("/updateusecase")
async def update_usecase(request_data: Request, db: Session = Depends(get_db)):
    """
    Update the model_version value for a specific usecase in the database.

    Parameters:
        - request_data: A dictionary containing the request data sent as JSON in the request body.
                        It should have the following keys:
                        - client: The client of the usecase to be updated.
                        - usecase: The usecase to be updated.
                        - model_version: The new model_version value.

        - db: The SQLAlchemy database session.

    Returns:
        A dictionary containing the response message.

    """

    try:
        # Extract the parameters from the request data
        json_data = await extract_json(request_data)
        usecase_data = UsecaseData(**json_data)
        
        print('%%%%%%% Update model version json data = ', json_data)
        # Get the Usecase class from the SQLAlchemy Base
        Usecase = Base.classes.usecases

        # Query the database for the matching usecase
        usecase = db.query(Usecase).filter(Usecase.client == usecase_data.client, Usecase.usecase == usecase_data.usecase).first()
        # Update the usecase data model_version attribute
        if usecase:
            # Update the model_version attribute
            usecase.model_version = usecase_data.model_version
            usecase.model_date = usecase_data.model_date
            db.commit()
            return {"message": "Usecase updated successfully"}
        else:
            return {"message": "Usecase not found"}

    except Exception as err:
        return {"Error message": str(err)}
    
# Add a new usecase class affecation to the database
# Define the model for the data to be inserted
class UsecaseClassData(BaseModel):
    client: str
    usecase: str
    id_class: int
    order_class: int = 0
    class_name: str = ''

    
@app.post("/addusecaseclass")

# async def addusecase(request: Request):
async def addusecaseclass(request: Request, db: Session = Depends(get_db)):
    """
     Add usecaseclass to database. This will be called by ajax call and it will return JSON data to be sent to client
     
     Args:
     	 request: request object with data to add
     	 db: database object to use in database request. data should be json
    """
    json_data = await extract_json(request)
    usecaseclass_data = UsecaseClassData(**json_data)
    print('usecaseclass_data = ', usecaseclass_data)
    try:
        UsecaseClass = Base.classes.usecase_class
        next_order = db.query(func.max(UsecaseClass.order_class).label("max_order")).filter(
            UsecaseClass.client == usecaseclass_data.client,
            UsecaseClass.usecase == usecaseclass_data.usecase
        ).one().max_order
        print('next_order = ', next_order)
        Colonie = Base.classes.classes
        colonie_name = db.query(Colonie.class_name).filter(
            Colonie.id_class == usecaseclass_data.id_class
            ).one()[0]
        print('colonie_name = ', colonie_name)
        # Set next_order to the next order.
        if next_order is None:
            next_order = 0
        else:
            next_order = next_order + 1  # if max_order is None (when table is empty), use 0 as default value
        usecaseclass_data.order_class = next_order
        usecaseclass_data.class_name = colonie_name
        print('usecaseclass_data = ',usecaseclass_data)
        db_usecaseclass =  Base.classes.usecase_class(**usecaseclass_data.dict())
        db.add(db_usecaseclass)
        db.commit()
        db.refresh(db_usecaseclass)
        

        # Return a success message
        return {"message": "Usecase_class inserted successfully"}
    
    except Exception as err:
        
        return {"Error message": err}
    
# Add a new client/usecase to the database
# Define the model for the data to be inserted
class AlBatchData(BaseModel):
    id_usecase: int
    client: str=''
    usecase: str=''
    execution_date: datetime = currenttime
    status: str ='Pending'
@app.post("/addalbatch")
async def addalbatch(albatch_data: AlBatchData, db: Session = Depends(get_db)):
    """
     Add an active learning batch to the database. This will update the execution date to the current time
     
     Args:
     	 albatch_data: Data to be added to the database
     	 db: Database connection to be used for database operations.
     
     Returns: 
     	 Success or failure message to be sent to the client
    """
    
    try:
        # AlBatch = Base.classes.al_batches
        albatch_data.execution_date = datetime.now()
        db_albatch =  Base.classes.al_batches(**albatch_data.dict())
        db.add(db_albatch)
        Image = Base.classes.images
        # if albatch_data. status is finished update the model
        if albatch_data.status == "Finished":
            db.query(Image).filter(
                Image.client == albatch_data.client,
                Image.usecase == albatch_data.usecase,
                Image.is_trained == 0,
                Image.is_feedback == 1
            ).update({Image.is_trained: 1, Image.is_feedback: 0})
        
        db.commit()
        db.refresh(db_albatch)
        

        # Return a success message
        return {"message": "Active learning batch inserted successfully"}
    except mysql.connector.errors.IntegrityError as e:
        # Catch the duplicate key violation error
        return {"message": str(e)}
    except Exception as err:
      
        return {"Error message": err}


@app.get("/getnextalbach")

async def getNextAlBatch(db: Session = Depends(get_db)):
    """
     Get information about the next al batch. This is a database query that can be used to determine the most up - to - date batch of usecases that will be used for the next batch.
     
     Args:
     	 db: database to use for database queries. If not provided will use DEPENDS.
     
     Returns: 
     	 list of dicts with keys : idusecases client usecase model_img model_batch model_epochs model_size order_usecase
    """
    try:
        sql = """
        SELECT u.idusecases, 0 as time_diff, u.client, u.usecase, u.model_img, u.model_batch, u.model_epochs, u.model_size, u.order_usecase, u.model_version
        FROM usecases u
        WHERE (u.client, u.usecase) IN (
                SELECT DISTINCT client, usecase FROM images WHERE is_feedback = 1 GROUP BY client, usecase HAVING COUNT(is_feedback) > 1
            ) 
        AND (u.client, u.usecase) NOT IN (
                SELECT DISTINCT ab.client, ab.usecase FROM al_batches ab
            )
        UNION
        SELECT u.idusecases, MIN(TIMEDIFF(NOW(), b.execution_date)) AS time_diff, u.client, u.usecase, u.model_img, u.model_batch, u.model_epochs, u.model_size, u.order_usecase, u.model_version
        FROM al_batches b
        RIGHT JOIN usecases u ON u.client = b.client AND u.usecase = b.usecase
        WHERE (u.client, u.usecase) IN (
                SELECT DISTINCT client, usecase FROM images WHERE is_feedback = 1 GROUP BY client, usecase HAVING COUNT(is_feedback) > 1
            ) 
        AND (u.client, u.usecase) IN (
                SELECT i.client, i.usecase FROM images i WHERE i.date_time > ALL (
                    SELECT MAX(b.execution_date) FROM al_batches b WHERE b.client = i.client AND b.usecase = i.usecase
                )
            )
        GROUP BY u.idusecases, u.client, u.usecase, u.model_img, u.model_batch, u.model_epochs, u.model_size, u.order_usecase
        ORDER BY time_diff DESC, order_usecase ASC;

        """

        result = db.execute(text(sql))
        rows = result.fetchall()
        # Prints all the rows in the list
        for row in rows:
            print('row = ', row)
        
        # Check if any result exists
        print(rows)
        # Returns a dictionary of rows from the status column
        if rows is not None and len(rows) > 0:
            # Returns the Pending value of the status column.
            for row in rows:
                # Pending value exists in the status column.
                if 'Pending' in row:
                    return None  # 'Pending' value exists in the status column
            
            return rows[0]._mapping # return as dictionary from SQLAlchemy 1.4
          
        else:
            return None  # No rows returned
    except Exception as e:
        return {"Error message": str(e)}
@app.get("/getmodelname")
async def getModelNameUsecase(client:str , usecase:str, db: Session = Depends(get_db) ):
    """
    Endpoint to get the model name of a usecase based on the client and usecase id.
    """
    return datahelper.get_model_name(db, client, usecase)

@app.get("/modelinfo")

async def get_model_info(
    client: str = Query(None),
    usecase: str = Query(None),
    db: Session = Depends(get_db)
) -> List[dict]:
    """
     Get the number of images grouped by client usecase and feedback_images.
     
     Args:
     	 client: The client to filter by ( optional ).
     	 usecase: The usecase to filter by ( optional ).
     	 db: The database to use. Defaults to get_db.
     
     Returns: 
     	 A list of dictionaries representing the query result. The keys are the client the values are the images
    """
    

    # Base SQL query
    sql = """
    # SELECT client, usecase, count(*) total_images, sum(is_feedback) feedback_images 
    # FROM microteksaldb.images
    SELECT u.client as client, u.usecase as usecase, count(*) total_images, sum(is_feedback) feedback_images , model_version  , model_date
    FROM microteksaldb.images i
     join microteksaldb.usecases u on i.client = u.client and i.usecase=u.usecase 
    """
    
    # List to hold conditions and their values
    conditions = []
    values = {}
    
    # Check if client is provided and add to conditions
    
    if client:
        conditions.append("u.client = :client")
        values["client"] = client
        
    # Check if usecase is provided and add to conditions
    
    if usecase:
        conditions.append("u.usecase = :usecase")
        values["usecase"] = usecase
        
    # If there are any conditions, add them to the SQL query
    # Add conditions to the SQL statement.
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    
    # Add the GROUP BY clause
    sql += " GROUP BY u.client, u.usecase,u.model_version, u.model_date;"

    # Execute the SQL query with the conditions
    result = db.execute(text(sql), values)
    rows = result.fetchall()

    # Convert the rows to a list of dictionaries
    results_list = [
        {"client": row[0], "usecase": row[1], "total_images": row[2], "feedback_images": row[3], "model_version": row[4], "model_date": row[5]}
        for row in rows
    ]

    return results_list

# Get usecase detail
@app.get("/getusecasedetail")

async def get_usecase_detail(
    client: str ,
    usecase: str ,
    db: Session = Depends(get_db)
) : 
    """
     Get the detail of a usecase. This is a function that will be used in conjunction with get_usesceses
     
     Args:
     	 client: The client to filter by
     	 usecase: The usecase to filter by. Must be in the usesceses table
     	 db: The database connection to use
     
     Returns: 
     	 A dict with the detail of the usecase or an error
    """
    
    try:
        Usecase = Base.classes.usecases
        row = db.query(Usecase).filter(
            Usecase.client == client, 
            Usecase.usecase == usecase
            ).one()
        return row
    except Exception as e:
        return {"message": str(e)}


@app.get("/modelhistory")
async def get_model_history(client: str, usecase: str, db: Session = Depends(get_db)):
    Hist_usecase_models = Base.classes.hist_usecase_models
    Usecase = Base.classes.usecases  # Assuming this is your Usecase model

    # Query from hist_usecase_models
    hist_query = db.query(
        Hist_usecase_models.client.label("client"),
        Hist_usecase_models.usecase.label("usecase"),
        Hist_usecase_models.model_version.label("model_version"),
        Hist_usecase_models.model_date.label("model_date")
    ).filter(
        Hist_usecase_models.client == client,
        Hist_usecase_models.usecase == usecase
    ).order_by(
        cast(Hist_usecase_models.model_version, Integer).desc()
        )

    

    # Execute the  query
    results = db.execute(hist_query).fetchall()

    if not results:
        raise HTTPException(status_code=404, detail="No model history found")

    # Convert results into a list of dictionaries
    model_history = [{"model_version": row.model_version, "model_date": row.model_date} for row in results]

    return model_history

@app.get("/copyusecase")
async def copy_usecase(client_src: str, usecase_src: str, client_dest: str, usecase_dest: str ,db: Session = Depends(get_db), doImageCopy: bool = True):
    """
     Copy a usecase from one client to another. This is a wrapper around datahelper. copy_model () to avoid having to create a model in the data - bucket
     
     Args:
     	 client_src: Name of the client that owns the usecase
     	 usecase_src: Name of the source usecase
     	 client_dest: Name of the client that owns the usecase
     	 usecase_dest: Name of the destination usecase
     	 db: Database connection to use case is in. Defaults to Depends
     	 doImageCopy: If True will copy images instead of
    """  
    
    try:
        source_bucket = 'microteks'
        source_folder = f'data-images-al/{client_src}/{usecase_src}/raw-data/'
        model_name_src=datahelper.get_model_name(db, client_src, usecase_src)
        destination_bucket = 'microteks'
        destination_folder = f'data-images-al/{client_dest}/{usecase_dest}/raw-data/'
        print(usecase_dest)
        # This function is used to get the model name from the usecase_dest
        if "SPORE_" in usecase_dest:
            print("contains SPORE_")
            model_name_dest = f'model_{client_dest[:3]}_{usecase_dest.split("_")[0]}_{usecase_dest.split("_")[1][:3]}{usecase_dest.split("_")[1][-2:]}'
        else:
            print("NO SPORE_")
            model_name_dest = f'model_{client_dest[:3]}_{usecase_dest[:3]}{usecase_dest[-2:]}'
        # Copy images from source to destination usecase
        if doImageCopy :
            # Copy images and labels from source to destination usecase
            print("copy images and labels folders")
            await datahelper.copy_folder(source_bucket=source_bucket,source_folder=source_folder,destination_bucket=destination_bucket,destination_folder=destination_folder)
            #  copy images tables records
            print("copy images in database")
            datahelper.copy_images_db(db, client_src, usecase_src, client_dest,usecase_dest)
        
        # Copy model
        model_src_file = f"model-deploy/{model_name_src}.pt"
        model_dest_file = f"model-deploy/{model_name_dest}.pt"
        print(" copy model file")
        await datahelper.copy_file_to_folder(model_src_file,model_dest_file, source_bucket = source_bucket,destination_bucket= destination_bucket)
        print("add model to api")
        await datahelper.add_model(client_dest, usecase_dest, model_name_dest)
        print("sync model ")
        await datahelper.sync_model(model_name_dest)
        print("update model name")
        await datahelper.update_model_name(db,client_dest,usecase_dest, model_name_dest)
        # copy mlflow production run 
        print("copy model name")
        source_exp_name = f'{client_src}_{usecase_src}'
        target_exp_name = f'{client_dest}_{usecase_dest}'
        source_model_name = source_exp_name
        target_model_name = target_exp_name
        await datahelper.copy_run(source_exp_name, target_exp_name, source_model_name, target_model_name)
        return {"Model duplicated succefully"}
        
    except Exception as err:
        
        return {"Error message during model duplicate": err}
    
