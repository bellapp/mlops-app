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
class NewTerminalBatchOperator(BaseOperator):
    @apply_defaults
    def __init__(self, command, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.command = command

    def execute(self, context):
        # Get the default terminal emulator command based on the OS
        print('sys.platform = ', sys.platform)
        if sys.platform.startswith('win'):
            terminal_cmd = 'start cmd /c'
        elif sys.platform.startswith('darwin'):
            terminal_cmd = 'open -a Terminal'
        else:
            terminal_cmd = 'xterm'

        
        subprocess.Popen(['/bin/bash', self.command])
        
default_args = {
    'owner': 'abdelaaziz',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

URL_ROOT = "http://ip_address:port/"
LOCAL_MODEL_FOLDER= '/home/forge/microteks-al/model'
def getnextusecase(ti):
    url= URL_ROOT + "getnextalbach"
    response = requests.get(url)
        # Check the response
    data = None

    if response.status_code == 200 :
        # Request was successful
        data = response.json()
        print('data = ', data)
        if data is None:
            return 'end_task'
        # print("data['idusecases'] = ",data['idusecases'])
        # print("data['idusecases'] = ",data[0])
        ti.xcom_push(key='idusecases', value=data['idusecases'])
        ti.xcom_push(key='client', value=data['client'])
        ti.xcom_push(key='usecase', value=data['usecase'])
        ti.xcom_push(key='model_img', value=data['model_img'])
        ti.xcom_push(key='model_batch', value=data['model_batch'])
        ti.xcom_push(key='model_epochs', value=data['model_epochs'])
        ti.xcom_push(key='model_size', value=data['model_size'])

        # return data
        # print(data)
        
    else:
        # Request failed
        
        print("Request failed with status code:", response.status_code)
        return 'end_task'
        # return None
    return 'test_feedback_count_task' if data is not None else 'end_task'

def test_feedback_count_task(ti):
    url= URL_ROOT + "getcountfeedbackimages"
    client = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='client')
    usecase = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='usecase')
    params = {
    "client": client,
    "usecase": usecase,
    }
    response = requests.get(url,params)
        # Check the response
    
    if response.status_code == 200:
        # Request was successful
        feedback_count = response.json()
        ti.xcom_push(key='feedback_count', value=feedback_count)
        print('## get feedback count = ',feedback_count)
        # return data
    else:
        # Request failed
        
        print("Request failed with status code:", response.status_code)
        # return None
    if feedback_count > 1 :
        return 'test_al_validation_task'
    else : 
        ti.xcom_push(key='batch_status', value='Skipped')
        return 'create_batch_task'
    # return 'train_task' if data is not None else 'end'

def test_al_validation_task(ti):
    is_verify_al_conditions = False
    is_check_priority = False
    client = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='client')
    usecase = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='usecase')
    
    if (data_validation.check_priority(client, usecase)):
        is_check_priority = True
        print('is_check_priority = ', is_check_priority)
        return 'prepare_data_task'
    if (data_validation.verify_al_conditions(client, usecase)):
        is_verify_al_conditions = True
        print('is_verify_al_conditions = ', is_verify_al_conditions)
        return 'prepare_data_task'
    
    # if is_verify_al_conditions or is_check_priority :
    #     return 'train_task'
    # else : 
    ti.xcom_push(key='batch_status', value='Skipped')
    return 'create_batch_task'


def create_batch_task(ti):
    next_task = 'end_task'
    batch_status = ti.xcom_pull(task_ids=['test_feedback_count_task','test_al_validation_task','prepare_data_task','train_spotty_task'], key='batch_status')
    idusecases = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='idusecases')
    client = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='client')
    usecase = ti.xcom_pull(task_ids='get_next_usecase_to_train', key='usecase')
    print('batch_status == ', batch_status)
    if len(batch_status)==0:
        print(' #### No batch created')
        return next_task
    if isinstance(batch_status, LazyXComAccess):
        print(batch_status[0])
        batch_status = batch_status[0]
        # batch_status = batch_status.get(key='batch_status')
   
    
        # Check the response
    
    
    if batch_status == 'Finished':
        os.environ['LOCAL_MODEL_FOLDER'] = LOCAL_MODEL_FOLDER
        import deploy
        model_version = deploy.run_deploy_process(client,usecase)
        if model_version is not None:
            print('model_version to deploy = ',model_version )
            model_version_result = 'V-'+ model_version
            print('Model veriosn of new production model = ', model_version_result)
            ti.xcom_push(key='model_version', value=model_version_result)
            print(' #### run sync_data_to_do_task after deploy')
            # update production model version in database
            print(' #### update model version in database')
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
    print('batch_status before insert =', batch_status)
    print('next_task is =', next_task)
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
    if response.status_code == 200:
         print("Batch created succefully")          
        
    else:
        # Request failed
        print("Request failed with status code:", response.text)
        # return None
    return next_task
     
def end_task():
    
    print('No next batch to train')
    return 'stop_spotty_task'    

def prepare_data_success_callback(context):
    prepare_data_variable = 'Prepare_data_Success'
    ti = context['task_instance']
    ti.xcom_push(key='prepare_data_variable', value=prepare_data_variable)
    # ti.xcom_push(key='batch_status', value='Finished')
    dag = context['dag']
    create_batch_task = dag.get_task('train_spotty_task')  # Replace with the actual target task ID
    create_batch_task.set_upstream(ti.task)

def prepare_data_failure_callback(context):
    prepare_data_variable = 'Prepare_data_Failure!'
    ti = context['task_instance']
    ti.xcom_push(key='prepare_data_variable', value=prepare_data_variable)

def spotty_success_callback(context):
    train_variable = 'Train_Success'
    ti = context['task_instance']
    ti.xcom_push(key='train_variable', value=train_variable)
    ti.xcom_push(key='batch_status', value='Finished')
    dag = context['dag']
    # stop_spotty_task = dag.get_task('stop_spotty_task')  # Replace with the actual target task ID
    # stop_spotty_task.set_upstream(ti.task)
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
# def stop_spotty_callback(**kwargs,context):
def stop_spotty_callback(ti):
    # subprocess.call("(ls /home/forge/spotty)", shell=True)
    cmd = "cd  /home/forge/spotty && /home/forge/microteks-al-venv/bin/spotty stop"
    returned_output = subprocess.check_output(cmd, shell=True, universal_newlines=True)

    print(returned_output)
    # ti = context['task_instance']
    # ti.xcom_push(key='batch_status', value='Finished')
    # dag = context['dag']
    # create_batch_task = dag.get_task('create_batch_task')  # Replace with the actual target task ID
    # create_batch_task.set_upstream(ti.task)
    return
def sync_data_success_callback(context):
    # train_variable = 'Train_Success'
    ti = context['task_instance']
    # ti.xcom_push(key='train_variable', value=train_variable)
    # ti.xcom_push(key='batch_status', value='Finished')
    dag = context['dag']
    # stop_spotty_task = dag.get_task('stop_spotty_task')  # Replace with the actual target task ID
    # stop_spotty_task.set_upstream(ti.task)
    stop_spotty_task = dag.get_task('stop_spotty_task')  # Replace with the actual target task ID
    stop_spotty_task.set_upstream(ti.task) 
    # return  'create_batch_task'

    # sp = subprocess.Popen(['/bin/bash','ls /home/forge/spotty'], 
    # stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    # print(sp.stdout.read())

start_date=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)



with DAG(
    default_args=default_args,
    dag_id='microteks_al_dag_v03',
    description='Microteks Active learning',
    # start_date=datetime(2023, 7, 6),
    start_date=start_date,
    schedule_interval='@daily'
    # schedule_interval='0 */6 * * *',
   
) as dag:
    # for i in range(2):
    task1 = BranchPythonOperator(
        task_id='get_next_usecase_to_train',
        python_callable=getnextusecase,
        # op_kwargs={'some_dict': {'a': 1, 'b': 2}}
    )
    # task2 = PythonOperator(
    #     task_id='get_name',
    #     python_callable=greet
    # )
    test_feedback_count_task = BranchPythonOperator(
        task_id='test_feedback_count_task',
        python_callable=test_feedback_count_task,
    )
    test_al_validation_task = BranchPythonOperator(
        task_id='test_al_validation_task',
        python_callable=test_al_validation_task,
    )
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
        # bash_command="echo 'Hello word'",
        on_success_callback=prepare_data_success_callback,
        on_failure_callback=prepare_data_failure_callback,
        do_xcom_push=True,
        trigger_rule=TriggerRule.ALL_SUCCESS # Modify trigger rule if needed
        # ,spotty_task
        # cwd='/home/forge/microteks-al'
        
    )
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
        # bash_command="echo 'Hello word'",
        on_success_callback=spotty_success_callback,
        on_failure_callback=spotty_failure_callback,
        do_xcom_push=True,
        trigger_rule=TriggerRule.ALL_SUCCESS, # Modify trigger rule if needed
        retries=0,  # Maximum number of retries for the task (overrides DAG's default)
        retry_delay=timedelta(minutes=5)  # Delay between retries for the task (overrides DAG's default)
        # ,
        # cwd='/home/forge/microteks-al'
        
    )
    # train_task = BashOperator(
    #     task_id='train_task',
    #     bash_command="""
    #     cd /home/forge/spotty &&\
    #     /home/forge/microteks-al-venv/bin/spotty run train        
    #     """,
    #     # bash_command="echo 'Hello word'",
    #     on_success_callback=train_success_callback,
    #     on_failure_callback=train_failure_callback,
    #     do_xcom_push=True,
    #     trigger_rule=TriggerRule.ALL_SUCCESS # Modify trigger rule if needed
    #     # ,
    #     # cwd='/home/forge/microteks-al'
        
    # )
    # train_task = NewTerminalBatchOperator(
    #     task_id='train_task',
    #     command="""
    #     cd /home/forge/spotty &&\
    #     /home/forge/microteks-al-venv/bin/spotty run train        
    #     """,
    #     # bash_command="echo 'Hello word'",
    #     on_success_callback=train_success_callback,
    #     on_failure_callback=train_failure_callback,
    #     do_xcom_push=True,
    #     trigger_rule=TriggerRule.ALL_SUCCESS # Modify trigger rule if needed
    # )
    stop_spotty_task = PythonOperator(
        task_id='stop_spotty_task',
        # provide_context=True,
        # do_xcom_push=True,
        python_callable=stop_spotty_callback,
        retries=2,
        # trigger_rule=TriggerRule.ONE_SUCCESS
        trigger_rule=TriggerRule.ALL_DONE)
    create_batch_task = BranchPythonOperator(
        task_id='create_batch_task',
        python_callable=create_batch_task,
        trigger_rule=TriggerRule.NONE_FAILED
    )
    sync_data_to_do_task = BashOperator(
        task_id='sync_data_to_do_task',
        # bash_command="""
        # export CLIENT={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='client') }} &&\
        # export USECASE={{ ti.xcom_pull(task_ids='get_next_usecase_to_train',key='usecase') }} &&\
        # echo 'client ' $CLIENT &&\
        # echo 'usecase ' $USECASE &&\
        # cd /home/forge/microteks-al &&\
        
        # """,
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
        # s3cmd --config /home/forge/.s3cfg sync  data_model/ s3://microteks/data-images-al/$CLIENT/$USECASE/ --recursive --force  --delete-removed &&\
        # bash_command="echo 'Hello word'",
        # on_success_callback=spotty_success_callback,
        # on_failure_callback=spotty_failure_callback,
        # do_xcom_push=True,
        # trigger_rule=TriggerRule.ALL_SUCCESS # Modify trigger rule if needed
        # ,
        # cwd='/home/forge/microteks-al'
        
    )
    none_task = PythonOperator(
        task_id='end_task',
        python_callable=end_task,
    )


    task1 >> [test_feedback_count_task ,none_task]
    test_feedback_count_task >> [test_al_validation_task,create_batch_task]
    test_al_validation_task >> [prepare_data_task,create_batch_task]
    prepare_data_task >> [none_task,train_spotty_task]
    # train_spotty_task >> stop_spotty_task
    # stop_spotty_task >> [create_batch_task]
    train_spotty_task >> create_batch_task
    create_batch_task >> [sync_data_to_do_task,none_task]
    [sync_data_to_do_task,none_task] >> stop_spotty_task
    # ,create_batch_task