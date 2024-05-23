import mysql.connector
import pandas as pd
from pydantic import BaseSettings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import or_, text
from sqlalchemy.sql import func
from contextlib import contextmanager

import mlflow
from mlflow.entities import ViewType

mydb = mysql.connector.connect(
    
    host ="",
    # host=host,
    port="" ,
    user="",
    
    password="",
    database=""
)
class Settings(BaseSettings):
    app_name: str = "Awesome API"
    env_param: str = ""
    
    
settings = Settings()
if settings.env_param == "development":
    # Set parameters for development mode
    host =""
else:
    # Set parameters for production mode
    host =""
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://host/db_name"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Dependency
@contextmanager
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
Base = automap_base()
Base.prepare(engine, reflect=True)

# Initiate Vvariable for active learning condition rates
NEW_IMAGE_RATE=0.1
THRESHOLD_IMAGE_CLASS_RATE = 0.5
THRESHOLD_IMAGE_INSTANCE_RATE = 0.2




def verify_al_conditions(client:str, usecase:str):
    """
     Verify conditions for Active Learning all verified before retraining the model
     
     Args:
     	 client: The client for whom the usecase is being ordered
     	 usecase: The usecase for whom the client is being ordered
     
     Returns: 
     	 True if there is a condition that is met False
    """
    val=(client, usecase)
    print(client, usecase)
    
    # Calculate the next available value for the order column
    sql_total_image= """SELECT count(*) FROM images i where i.client=%s and i.usecase=%s 
    
    """
    
    mycursor = mydb.cursor()
    mycursor.execute(sql_total_image, val)
    total_image = mycursor.fetchone()[0]
    print('total_images = ',total_image)
    sql_total_class= "SELECT count(*) FROM usecase_class i where i.client=%s and i.usecase=%s "
    
    mycursor.execute(sql_total_class, val)
    total_class = mycursor.fetchone()[0]
    print('total_class = ',total_class)

    # Condition 1
    print("######## Condition 1 ########")
    new_images_threshold = total_image* NEW_IMAGE_RATE
    print('new_images_threshold = ', new_images_threshold)
    
    

    sql_new_image_class=""" SELECT client, usecase,is_feedback, JSON_EXTRACT(class_obj, '$.class') AS class,
       count(JSON_EXTRACT(class_obj, '$.count')) AS class_new_images
        FROM images,
            JSON_TABLE(class_count, '$[*]'
                        COLUMNS (class_obj JSON PATH '$')) AS jt
        WHERE client=%s and usecase=%s and  is_feedback=1
        GROUP BY client, usecase, class,is_feedback ;
        """
    
    mycursor.execute(sql_new_image_class, val)
    result3 = mycursor.fetchall()
    # Convert the result to a DataFrame
    columns = [desc[0] for desc in mycursor.description]  # Get the column names from the cursor
    df_new_image_class = pd.DataFrame(result3, columns=columns)
    print('df_new_image_class = \n',df_new_image_class)
    
    condition_1 = (df_new_image_class['class_new_images'] >= new_images_threshold).all()
    print('condition_1 = ', condition_1)

    # Condition 2
    print("######## Condition 2 ########")
    sql_total_image_class=""" SELECT client, usecase, JSON_EXTRACT(class_obj, '$.class') AS class,
       count(JSON_EXTRACT(class_obj, '$.count')) AS class_total_images
        FROM images,
            JSON_TABLE(class_count, '$[*]'
                        COLUMNS (class_obj JSON PATH '$')) AS jt
        WHERE client=%s and usecase=%s 
        GROUP BY client, usecase, class ;
        """
    mycursor.execute(sql_total_image_class, val)
    result_total_image_class = mycursor.fetchall()
    # Convert the result to a DataFrame
    columns = [desc[0] for desc in mycursor.description]  # Get the column names from the cursor
    df_total_image_class = pd.DataFrame(result_total_image_class, columns=columns)
    print('df_total_image_class = \n',df_total_image_class)
    image_class_threshold_number = (total_image/total_class)*THRESHOLD_IMAGE_CLASS_RATE
    print('image_class_threshold_number = ', image_class_threshold_number)
    condition_2 = (df_total_image_class['class_total_images'] >= image_class_threshold_number).all()
    print('condition_2 = ', condition_2)

    # Condition 3
    print("######## Condition 3 ########")
    sql_total_instance= """
    SELECT client, usecase, JSON_EXTRACT(class_obj, '$.class') AS class,
       SUM(JSON_EXTRACT(class_obj, '$.count')) AS instance_total_images
        FROM images,
            JSON_TABLE(class_count, '$[*]'
                        COLUMNS (class_obj JSON PATH '$')) AS jt
        where client=%s and usecase=%s
        GROUP BY client, usecase, class;
    """
    
    mycursor = mydb.cursor()
    mycursor.execute(sql_total_instance, val)
    result_image_instance=mycursor.fetchall()
    # Convert the result to a DataFrame
    columns = [desc[0] for desc in mycursor.description]  # Get the column names from the cursor
    df_total_image_instance = pd.DataFrame(result_image_instance, columns=columns)
    print('df_total_image_instance = \n',df_total_image_instance)

    # Get the sum of count_sum where is_trained is 1
    total_instance = df_total_image_instance['instance_total_images'].sum()
    print('total_instance = ' , total_instance)
    image_instance_threshold_number = (total_instance/total_class)*THRESHOLD_IMAGE_INSTANCE_RATE
    print('image_instance_threshold_number = ', image_instance_threshold_number)

    # Convert the result to an integer
    condition_3 = (df_total_image_instance['instance_total_images'] >= image_instance_threshold_number).all()
    print('condition_3 = ', condition_3)
    


    mycursor.close()
    mydb.close()
    # run_active_learning = condition_1 * condition_2 * condition_3
    run_active_learning = condition_1 * condition_2 
    print('run_active_learning =' , run_active_learning)
    return run_active_learning




def check_priority(client:str, usecase:str):
    """
     Check if the class within high priority have feedback images for the given client, usecase
     
     Args:
     	 client: The client that is going to run the experiment
     	 usecase: The usecase that is going to run the experiment
     
     Returns: 
     	 True if there is a classwithin high priority have feedback images 
    """
    # Set the experiment name
    experiment_name = client +"_"+usecase
    print('experiment_name =', experiment_name)
    # Connect to the MLflow tracking server
    mlflow.set_tracking_uri("http://active-learning.microteks.io:5000")  # Set your MLflow tracking server URI

    # Get the experiment ID
    # Returns True if the experiment is found in the mlflow.
    if mlflow.get_experiment_by_name(experiment_name) is None:
        return False
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
    print(experiment_id)

    # Get the runs in the experiment
    runs = mlflow.search_runs(experiment_ids=[experiment_id],  filter_string="status='FINISHED'",order_by=["start_time desc"], max_results=1)
    print(runs)
    # Get the last run ID
    last_run_id = runs.iloc[0]["run_id"]

    # Fetch the metrics from the last run
    metrics = mlflow.get_run(last_run_id).data.metrics
    print(metrics)
    class_metrics = {}
    # Add a metric to the class_metrics dictionary.
    for key, value in metrics.items():
        # Add a metric to the class_metrics map.
        if '/' in key:
            class_name, metric_name = key.split('/', 1)
            # Add a class to the class_metrics map.
            if class_name not in class_metrics:
                class_metrics[class_name] = {}
            class_metrics[class_name][metric_name] = value

    # Create DataFrame from class-specific metrics
    df = pd.DataFrame(class_metrics).T.reset_index().rename(columns={'index': 'class'})
 
    df = df.set_index('class').T

    priority_classes = []

    # Sort the DataFrame by the total of metrics in descending order
    df_sorted = df.assign(total=df.sum(axis=1)).sort_values('total', ascending=False)
    df_sorted = df_sorted.drop('total', axis=1)

    # Add priority classes to priority_classes list
    for i in range(1, len(df_sorted)):
        measures_prev = df_sorted.iloc[i-1]
        measures_current = df_sorted.iloc[i]

        count_below_threshold = sum(1 for x, y in zip(measures_prev, measures_current) if x - y >= 0.1)

        # Add priority classes to priority classes if count_below_threshold 2.
        if count_below_threshold >= 2:
            priority_classes.append(df_sorted.index[i])
   
   
    # Add priority classes to priority classes.
    for i in range(1, len(df_sorted)):
      # Add priority classes to priority_classes list
      if df_sorted.index[i-1] in priority_classes:
          priority_classes.append(df_sorted.index[i])

       
    print('priority_classes = ', priority_classes)
        
    feedbacklist = getlistclassfeedback(client, usecase)
    print('feedbacklist = ',feedbacklist)
    is_priorityclass_have_feedback = any(element in feedbacklist for element in priority_classes)

    
    return is_priorityclass_have_feedback

def getUsecaseClasses(client,usecase,db):
    """
     Get information about usecase classes. This is a helper function for get_uscase_classes.
     
     Args:
     	 client: The client to get information about. This is used to check if we have access to the client's data.
     	 usecase: The usecase to get information about.
     	 db: The database connection to use. This is used to get the class information.
     
     Returns: 
     	 A dictionary where the keys are the class IDs and the values are dictionaries with the following keys : id_class The ID of the class. order_class The order of the class. class_name The name of the class
    """
    
    UsecaseClass = Base.classes.usecase_class
    
    rows = (
            db.query(UsecaseClass.id_class, UsecaseClass.order_class, UsecaseClass.class_name)
            .filter(UsecaseClass.client == client, UsecaseClass.usecase == usecase  )
            .order_by(UsecaseClass.order_class.asc())
            .all()
            )
    
    result = {row.id_class: {"id_class": row.id_class, "order_class": row.order_class, "class_name": row.class_name} for row in rows}
    return result

def getlistclassfeedback(client:str, usecase:str):
    """
    Get a list of class objects that have feedback in a particular client and usecase
    
    Args:
        client: The client to get the list of feedback for
        usecase: The usecase to get the list of feedback for
    
    Returns: 
        A list of tuples class_name of feedback images
    """

    with get_db() as db:
        result=[]
        # Calculate the next available value for the order column
        
        sql1 = text("""SELECT client, usecase, JSON_EXTRACT(class_obj, '$.class') AS id_class
                FROM images,
                  JSON_TABLE(class_count, '$[*]'
                             COLUMNS (class_obj JSON PATH '$')) AS jt
                where client= :cl and usecase= :uc and is_feedback=1
                GROUP BY client, usecase, id_class;""")
        val1 = (client, usecase)
        result_total_image_class = db.execute(sql1, {"cl":client, "uc":usecase}).fetchall() 
     
        feedback_list = [(row.client, row.usecase, row.id_class) for row in result_total_image_class]

    
        print('feedback_list = ',feedback_list)
        # list of feedback_list 
        if len(feedback_list)>0:
            feedback_list = list(zip(*feedback_list))[2]
            print('feedback_list 2 = ',feedback_list)
            list_classes = getUsecaseClasses(client,usecase,db)
            print('list_classes = ', list_classes)
        
            result = [list_classes[int(i)]['class_name'] for i in feedback_list if int(i) in list_classes]
        return result
