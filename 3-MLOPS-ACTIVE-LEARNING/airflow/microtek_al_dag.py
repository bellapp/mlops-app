from datetime import datetime, timedelta
import requests
import data_validation
from airflow.models.xcom import LazyXComAccess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import subprocess
import sys
import os

# Dag argument
default_args = {
    'owner': 'abdelaaziz',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

URL_ROOT = "http://ip_address:port/"
LOCAL_MODEL_FOLDER= '/home/forge/microteks-al/model'
def getnextusecase(ti):
    """
     Gets the next usecase to train for Active Learning and adds it to XCom
     
     Args:
     	 ti: Instance of TaskManager to add task to.
     
     Returns: 
     	 None if success else error message to xcom_
    """
    url= URL_ROOT + "getnextalbach"
    response = requests.get(url)
    
    data = None

    # This function is called when the response is successful.
    if response.status_code == 200 :
        # Request was successful
        data = response.json()
        
        # Return the name of the task to be executed.
        if data is None:
            return 'end_task'
        # Put variable to Xcom
        ti.xcom_push(key='idusecases', value=data['idusecases'])
        ti.xcom_push(key='client', value=data['client'])
        ti.xcom_push(key='usecase', value=data['usecase'])
        ti.xcom_push(key='model_img', value=data['model_img'])
        ti.xcom_push(key='model_batch', value=data['model_batch'])
        ti.xcom_push(key='model_epochs', value=data['model_epochs'])
        ti.xcom_push(key='model_size', value=data['model_size'])
    else:
        print("Request failed with status code:", response.status_code)
        return 'end_task'
        
    return 'test_feedback_count_task' if data is not None else 'end_task'

def test_feedback_count_task(ti):
    """
     Tests the get count feedback images task. It is used to get the number of images that have been submitted to the task_ids and validate them
     
     Args:
     	 ti: Instance of the TrainInfo class
     
     Returns: 
     	 None if request was successful otherwise error message to x
    """
    url= URL_ROOT + "getcountfeedbackimages"
    client = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='client')
    usecase = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='usecase')
    params = {
    "client": client,
    "usecase": usecase,
    }
    response = requests.get(url,params)

    # Get feedback count from XCom.
    if response.status_code == 200:
        # Request was successful
        feedback_count = response.json()
        ti.xcom_push(key='feedback_count', value=feedback_count)
        print('## get feedback count = ',feedback_count)
        # return data
    else:
        # Request failed
        return None
    # test_al_validation_task test_al_validation_task create_batch_task test_al_validation_task test_al_validation_task create_batch_task test_al_validation_task create_batch_task create_batch_task test_al_validation_task create_batch_task create_batch_task create_batch_task create_batch_task create_batch_task create_batch_task create_batch_task create_batch_task create_batch_task create_batch_task test_al_batch_task test_al_batch_task test_al_validation_task create_batch_task test_batch_task
    if feedback_count > 1 :
        return 'test_al_validation_task'
    else : 
        ti.xcom_push(key='batch_status', value='Skipped')
        return 'create_batch_task'

def test_al_validation_task(ti):
    """
     Tests data validation task. This is a test function to be run in a task runner
     
     Args:
     	 ti: Instance of XCom class.
     
     Returns: 
     	 String to be passed to XCom's run
    """
    is_verify_al_conditions = False
    is_check_priority = False
    client = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='client')
    usecase = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='usecase')
    
    # Check if the data task is valid.
    if (data_validation.check_priority(client, usecase)):
        is_check_priority = True
        print('is_check_priority = ', is_check_priority)
        return 'prepare_data_task'
    # prepare_data_task is the task to prepare the data task
    if (data_validation.verify_al_conditions(client, usecase)):
        is_verify_al_conditions = True
        print('is_verify_al_conditions = ', is_verify_al_conditions)
        return 'prepare_data_task'
    
    ti.xcom_push(key='batch_status', value='Skipped')
    return 'create_batch_task'


def create_batch_task(ti):
    """
     Create and return a batch task. 
     
     Args:
     	 ti: Instance of TaskManager to use. Must have access to tasks and usecases
     
     Returns: 
     	 String to be added to xcom_
    """
    next_task = 'end_task'
    batch_status = ti.xcom_pull(task_ids=['test_feedback_count_task','test_al_validation_task','prepare_data_task','train_spotty_task'], key='batch_status')
    idusecases = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='idusecases')
    client = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='client')
    usecase = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='usecase')
    print('batch_status == ', batch_status)
    # This function is called when a batch is created
    if len(batch_status)==0:
        print(' #### No batch created')
        return next_task
    # Check the batch status of the batch_status.
    if isinstance(batch_status, LazyXComAccess):
        print(batch_status[0])
        batch_status = batch_status[0]
    
    
    # This method is called by the deploy process and updates the model version.
    if batch_status == 'Finished':
        os.environ['LOCAL_MODEL_FOLDER'] = LOCAL_MODEL_FOLDER
        import deploy
        model_version = deploy.run_deploy_process(client,usecase)
        # deploy the production model version to the server
        if model_version is not None:
            print('model_version to deploy = ',model_version )
            model_version_result = 'V-'+ model_version
            print('Model veriosn of new production model = ', model_version_result)
            ti.xcom_push(key='model_version', value=model_version_result)
            # update production model version in database
            params_model = {
                "client": client,
                "usecase": usecase,
                "model_version": model_version,
                "model_date": datetime.now().isoformat()
            }
            url= URL_ROOT + "updateusecase"
            response = requests.put(url, json=params_model)
            next_task = 'sync_data_to_do_task'
        else:
            batch_status = 'Skipped'
            next_task = 'end_task'
    # insert batch
    params = {
    "client": client,
    "usecase": usecase,
    "status": batch_status,
    "execution_date": datetime.now().isoformat(),
    "id_usecase": idusecases
    }
    print("create_batch_task params = ", params)
    url= URL_ROOT + "addalbatch"
    response = requests.post(url, json=params)
    # Batch created succefully or None if the response code is 200.
    if response.status_code == 200:
         print("Batch created succefully")          
        
    else:
        # Request failed
        print("Request failed with status code:", response.text)
        
    return next_task
     
def end_task():
    
    print('No next batch to train')
    return 'stop_spotty_task'    

def prepare_data_success_callback(context):
    """
     Callback for success of Prepare_Data. This is called when the task is ready to be run.
     
     Args:
     	 context: A dictionary containing the following keys : task_instance : The task instance on which to run the task
    """
    prepare_data_variable = 'Prepare_data_Success'
    ti = context['task_instance']
    ti.xcom_push(key='prepare_data_variable', value=prepare_data_variable)
    # ti.xcom_push(key='batch_status', value='Finished')
    dag = context['dag']
    create_batch_task = dag.get_task('train_spotty_task')  # Replace with the actual target task ID
    create_batch_task.set_upstream(ti.task)

def prepare_data_failure_callback(context):
    """
     Callback for failure to prepare data. This is a temporary workaround to avoid running prepare_data on XCom when there is no connection to the host
     
     Args:
     	 context: Dictionary with the following keys : task_instance : The Task Instance to operate on
    """
    prepare_data_variable = 'Prepare_data_Failure!'
    ti = context['task_instance']
    ti.xcom_push(key='prepare_data_variable', value=prepare_data_variable)

def spotty_success_callback(context):
    """
     spotty_success_callback is called when the task has completed successfully. It will add a train variable to XCom and batch status to XCom
     
     Args:
     	 context: A dictionary containing the following keys : task_instance : The task instance to be trained
    """
    train_variable = 'Train_Success'
    ti = context['task_instance']
    ti.xcom_push(key='train_variable', value=train_variable)
    ti.xcom_push(key='batch_status', value='Finished')
    dag = context['dag']
    create_batch_task = dag.get_task('create_batch_task')  # Replace with the actual target task ID
    create_batch_task.set_upstream(ti.task)

def spotty_failure_callback(context):
    train_variable = 'Train_Success'
    ti = context['task_instance']
    ti.xcom_push(key='train_variable', value=train_variable)

def train_success_callback(context):
    train_variable = 'Train_Success'
    ti = context['task_instance']
    ti.xcom_push(key='train_variable', value=train_variable)
    ti.xcom_push(key='batch_status', value='Finished')
    dag = context['dag']
    create_batch_task = dag.get_task('create_batch_task')  # Replace with the actual target task ID
    create_batch_task.set_upstream(ti.task)

def train_failure_callback(context):
    train_variable = 'Train_Success'
    ti = context['task_instance']
    ti.xcom_push(key='train_variable', value=train_variable)

def stop_spotty_callback(ti):
    
    cmd = "cd  /home/forge/spotty && /home/forge/microteks-al-venv/bin/spotty stop"
    returned_output = subprocess.check_output(cmd, shell=True, universal_newlines=True)

    print(returned_output)
    return

def sync_data_success_callback(context):

    ti = context['task_instance']
    dag = context['dag']
    stop_spotty_task = dag.get_task('stop_spotty_task')  # Replace with the actual target task ID
    stop_spotty_task.set_upstream(ti.task) 

start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


# Start daily DAG
with DAG(
    default_args=default_args,
    dag_id='microteks_al_dag_v03',
    description='Microteks Active learning',
    start_date=start_date,
    schedule_interval='@daily'
   
) as dag:
    # Get nest usecase to be retrained for active learning
    get_next_usecase_to_train = BranchPythonOperator(
        task_id='get_next_usecase_to_train',
        python_callable=getnextusecase,
        
    )
    # Test if there is suffisant images feedback to start training
    test_feedback_count_task = BranchPythonOperator(
        task_id='test_feedback_count_task',
        python_callable=test_feedback_count_task,
    )
    # Test if all condition to run active learning are verified
    test_al_validation_task = BranchPythonOperator(
        task_id='test_al_validation_task',
        python_callable=test_al_validation_task,
    )
    # Preprare data by downloading them from s3 storage, set environment variables
    prepare_data_task = BashOperator(
        task_id='prepare_data_task',
        bash_command="""
        echo {{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='client') }} {{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='usecase') }} && \
        export CLIENT={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='client') }} &&\
        export USECASE={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='usecase') }} &&\
        export model_img={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='model_img') }} &&\
        export model_batch={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='model_batch') }} &&\
        export model_epochs={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='model_epochs') }} &&\
        export model_size={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='model_size') }} &&\
        echo 'client ' $CLIENT &&\
        echo 'usecase ' $USECASE &&\
        echo 'model_img   ' $model_img &&\
        echo $MLFLOW_S3_ENDPOINT_URL &&\
        cd /home/forge/microteks-al &&\
        rm -rf /home/forge/spotty/data_model/* &&\
        rm -rf data_model/* &&\
        source ./set_varaibles_env.sh &&\
        pwd &&\
        /home/forge/microteks-al-venv/bin/dvc repro  -f &&\
        cp -r data_model/* /home/forge/spotty/data_model/  &&\
        cd /home/forge/airflow/dags/ &&\
        /home/forge/microteks-al-venv/bin/python deploy.py $CLIENT $USECASE /home/forge/spotty/models -m download &&\
        echo 'client ' $CLIENT &&\
        echo 'usecase ' $USECASE &&\
        echo 'model_img 2  ' $model_img 
        """,
        
        on_success_callback=prepare_data_success_callback,
        on_failure_callback=prepare_data_failure_callback,
        do_xcom_push=True,
        trigger_rule=TriggerRule.ALL_SUCCESS # Modify trigger rule if needed
       
    )
    # Start AWS GPU Instance straining using Spotty
    train_spotty_task = BashOperator(
        task_id='train_spotty_task',
        bash_command="""
        export CLIENT={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='client') }} &&\
        export USECASE={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='usecase') }} &&\
        export model_img={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='model_img') }} &&\
        export model_batch={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='model_batch') }} &&\
        export model_epochs={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='model_epochs') }} &&\
        export model_size={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='model_size') }} &&\
        echo 'client ' $CLIENT &&\
        echo 'usecase ' $USECASE &&\
        echo 'model_img ' $model_img &&\
        cd /home/forge/spotty &&\
        /home/forge/microteks-al-venv/bin/python replace_spotty.py &&\
        /home/forge/microteks-al-venv/bin/spotty stop &&\
        echo 'y' | /home/forge/microteks-al-venv/bin/spotty start &&\
        /home/forge/microteks-al-venv/bin/spotty exec -- python yolov5/train.py --img $model_img --batch $model_batch --epochs $model_epochs --data data_model/raw-data/data.yaml --weights $model_size --cache --project output
    
        """,
        on_success_callback=spotty_success_callback,
        on_failure_callback=spotty_failure_callback,
        do_xcom_push=True,
        trigger_rule=TriggerRule.ALL_SUCCESS, # Modify trigger rule if needed
        retries=0,  # Maximum number of retries for the task (overrides DAG's default)
        retry_delay=timedelta(minutes=5)  # Delay between retries for the task (overrides DAG's default)
        
        
    )
    # Stop AWS EC2 instance after finishing training 
    stop_spotty_task = PythonOperator(
        task_id='stop_spotty_task',
        python_callable=stop_spotty_callback,
        retries=2,  
        trigger_rule=TriggerRule.ALL_DONE)
    # Crete Batch in database for the current usecase
    create_batch_task = BranchPythonOperator(
        task_id='create_batch_task',
        python_callable=create_batch_task,
        trigger_rule=TriggerRule.NONE_FAILED
    )
    # Push data and model to  repository using DVC
    sync_data_to_do_task = BashOperator(
        task_id='sync_data_to_do_task',
        bash_command="""
        export CLIENT={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='client') }} &&\
        export USECASE={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='usecase') }} &&\
        export MODEL_VERSION={{ ti.xcom_pull(task_ids='create_batch_task',key='model_version') }} &&\
        echo 'client ' $CLIENT &&\
        echo 'usecase ' $USECASE &&\
        echo 'model verion ' $MODEL_VERSION &&\
        cd /home/forge/microteks-al &&\
        s3cmd --config /home/forge/.s3cfg sync data_model/ s3://microteks/data-images-al/$CLIENT/$USECASE/ --recursive --force --delete-removed &&\
        echo '#### Running DVC commit for Data and Model ' &&\
        export GITHUB_URL=git@bitbucket.org:neuralteks/microteks-dvc.git
        export BRANCH_NAME=${CLIENT}_${USECASE} &&\
        export REPO_NAME=microteks-dvc &&\
        export S3_URL=s3://microteks &&\
        export TAG=${CLIENT}_${USECASE}_${MODEL_VERSION} &&\
        source dvc_automation.sh $GITHUB_URL $BRANCH_NAME $REPO_NAME $S3_URL $TAG 
        """,
        on_success_callback=sync_data_success_callback,
    )
    # THe final tast
    end_task = PythonOperator(
        task_id='end_task',
        python_callable=end_task,
    )


    get_next_usecase_to_train >> [test_feedback_count_task ,end_task]
    test_feedback_count_task >> [test_al_validation_task,create_batch_task]
    test_al_validation_task >> [prepare_data_task,create_batch_task]
    prepare_data_task >> [end_task,train_spotty_task]
    train_spotty_task >> create_batch_task
    create_batch_task >> [sync_data_to_do_task,end_task]
    [sync_data_to_do_task,end_task] >> stop_spotty_task
