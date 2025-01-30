import boto3
import os
from configparser import ConfigParser
from datetime import datetime
import time

dt_proc = datetime.now().strftime('%Y%m%d_%H%M%S')

class MalhaExtractionOperator:

    def __init__(self):
        self.__s3_client = self.__get_s3_client()

    def __get_client_credentials(self):
        config = ConfigParser()
        config_file_path = '/opt/airflow/config/aws_cliente.cfg'
        config.read_file(open(config_file_path))
        region = config.get('AWS_CLIENTE', 'region')
        aws_access_key_id = config.get('AWS_CLIENTE', 'aws_access_key_id')
        aws_secret_access_key = config.get('AWS_CLIENTE', 'aws_secret_access_key')

        return region, aws_access_key_id, aws_secret_access_key

    def __get_s3_client(self):
        region, aws_access_key_id, aws_secret_access_key = self.__get_client_credentials()

        return boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def __list_and_download_from_s3(self, bucket_name, prefix, local_path):

        try:
            response = self.__s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            if 'Contents' not in response:
                print(f"Nenhum arquivo encontrado no bucket '{bucket_name}' com prefixo '{prefix}'.")
                return

            for obj in response['Contents']:
                object_name = obj['Key']
                
                if object_name.endswith('/'):
                    continue            
                
                local_file_path = f"{local_path}/{object_name.split('/')[-1]}"

                print(f"Baixando {object_name} para {local_file_path}...")

                self.__s3_client.download_file(bucket_name, object_name, local_file_path)
                print(f"Arquivo {object_name} baixado com sucesso!")

        except Exception as e:
            print(f"Erro ao listar ou baixar arquivos: {e}")

    def start(self, bucket_name, prefix):
        """
        Parametros para passar dentro da DAG:

        bucket_name = bkt-edj-ped-datalake-dev
        prefix = ingestion/tb_faturas/        
        """
        local_path = '/tmp'
        
        self.__list_and_download_from_s3(bucket_name, prefix, local_path)


class MalhaLoadOperator:

    def __init__(self):
        self.__s3_lake = self.__get_s3_lake()

    def __get_lake_credentials(self):
        config = ConfigParser()
        config_file_path = '/opt/airflow/config/aws_lake.cfg'
        config.read_file(open(config_file_path))
        region = config.get('AWS_S3', 'region')
        aws_access_key_id = config.get('AWS_S3', 'aws_access_key_id')
        aws_secret_access_key = config.get('AWS_S3', 'aws_secret_access_key')

        return region, aws_access_key_id, aws_secret_access_key

    def __get_s3_lake(self):
        region, aws_access_key_id, aws_secret_access_key = self.__get_lake_credentials()

        return boto3.client('s3', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def __pick_filename(self, assunto, local_path):
        dados = os.listdir(local_path)
        for dado in dados:
            if dado.startswith(f'{assunto}'):
                return dado
        raise FileNotFoundError(f"Nenhum arquivo '{assunto}' encontrado em {local_path}!")
    
    def __upload_to_s3_data(self, filename, bucket_name, object_name):

        object_path = object_name + 'dados/' + filename.split('/')[-1]

        try:
            self.__s3_lake.upload_file(filename, bucket_name, object_path)
            print(f'Aquivo {filename} carregado em {bucket_name}/{object_name}')
        except Exception as e:
            print(f'Falha ao carregar {filename} em {bucket_name}/{object_name}. Error: {e}')

    def __upload_to_s3_control(self, filename, bucket_name, object_name):

        controle_filename = filename.split('.csv')[0] + '.ctl'
        data = []
        df = pd.DataFrame(data)
        df.to_csv(controle_filename)
        controle_path = object_name + 'controle/' + controle_filename

        try:
            self.__s3_lake.upload_file(controle_filename, bucket_name, controle_path)
            print(f"Arquivo {controle_filename} carregado em {bucket_name}/{object_name}")
            return True
        except Exception as e:
            print(f"Falha ao carregar {controle_filename} em {bucket_name}/{object_name}. Error: {e}")

    def start(self, assunto, bucket_name):
        
        local_path = '/tmp/'

        try:
            filename = self.__pick_filename(assunto, local_path)
        except FileNotFoundError as e:
            print(f"Error: {e}")
            return
        object_name = f'0001_raw/{assunto}/'

        self.__upload_to_s3_data(local_path + filename, bucket_name, object_name)

        self.__upload_to_s3_control(filename, bucket_name, object_name)


class MalhaProcessOperator:

    def __init__(self):
        self.__emr_client = self.__get_emr_client()
    
    def __get_credentials(self):
        config = ConfigParser()
        config_file_path = '/opt/airflow/config/aws_lake.cfg'
        config.read_file(open(config_file_path))
        region = config.get('AWS_S3', 'region')
        aws_access_key_id = config.get('AWS_S3', 'aws_access_key_id')
        aws_secret_access_key = config.get('AWS_S3', 'aws_secret_access_key')

        return region, aws_access_key_id, aws_secret_access_key

    def __check_controle_file(self, assunto):
        print(f'Verificando existencia de arquivos de controle de {assunto}')
        region, aws_access_key_id, aws_secret_access_key = self.__get_credentials()

        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        
        controle_path = f'0001_raw/{assunto}/controle/'

        print(f'Path Controle: {controle_path}')

        try:
            response = s3_client.list_objects_v2(Bucket='lake-project-', Prefix = controle_path)

            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.ctl'):
                        print(f'File found: {obj['Key'].split('/')[-1]}')
                        return obj['Key'].split('/')[-1]
            
            print('No .ctl file found in the specified folder.')
            return False

        except Exception as e:
            print(f'An error occurred: {e}')
            return False
    
    def __get_emr_client(self):
        region, aws_access_key_id, aws_secret_access_key = self.__get_credentials()

        return boto3.client('emr', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def __create_emr_cluster(self, assunto):
        print(f'Criando cluster emr')
        cluster_name = f'0001-trusted-{assunto}-lake-project'
        log_uri = 's3://lake-project-/0004_logs/trusted_process'
        release_label = 'emr-6.0.0'
        master_instance_type = 'm5.xlarge'

        response = self.__emr_client.run_job_flow(
            Name = cluster_name,
            LogUri = log_uri,
            ReleaseLabel = release_label,
            Instances = {
                    'InstanceGroups': [
                {
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1,
                },
            ]},
            Applications = [{'Name': 'Spark'}],
            ServiceRole = 'EMR_DefaultRole',
            JobFlowRole = 'EMR_EC2_DefaultRole'
        )

        print(f'Criado Cluster ID: {response['JobFlowId']}')

        cluster_id = response['JobFlowId']

        return cluster_id
    
    def __add_step_job(self, assunto, controle_file, cluster_id):
        print('Adicionando step job')
        step_name = f'process_lake_{assunto}'
        script_path = f's3://lake-project-/0005_scripts/trusted/{assunto}_process.py'

        print(script_path)
        print(controle_file)

        step = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode','client',script_path, controle_file],
            },
        }

        response = self.__emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps = [step])

        step_job_id = response['StepIds'][0]

        return step_job_id
    
    def __wait_step_job(self, step_id, cluster_id):
        while True:
            response = self.__emr_client.describe_step(ClusterId = cluster_id, StepId = step_id)
            state = response['Step']['Status']['State']

            if state in ['PENDING', 'RUNNING']:
                print(f'Executando step job...Estado: {state}')
                time.sleep(30)
            elif state == 'COMPLETED':
                print(f'Execução do step job finalizada...Estado: {state}')
                break
            else:
                raise Exception(f'O Step Job falhou com estado: {state}')

    def __terminate_emr_cluster(self, cluster_id):
        self.__emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    
    def start(self, assunto):
        controle_file = self.__check_controle_file(assunto)

        if controle_file == False:
            raise Exception(f'Arquivo de Controle não encontrado para assunto {assunto}')

        cluster_id = self.__create_emr_cluster(assunto)

        step_id = self.__add_step_job(assunto, controle_file, cluster_id)

        self.__wait_step_job(step_id, cluster_id)

        self.__terminate_emr_cluster(cluster_id)


class MalhaBookOperator:
    def __init__(self):
        self.__emr_client = self.__get_emr_client()
    
    def __get_credentials(self):
        config = ConfigParser()
        config_file_path = '/opt/airflow/config/aws_lake.cfg'
        config.read_file(open(config_file_path))
        region = config.get('AWS_S3', 'region')
        aws_access_key_id = config.get('AWS_S3', 'aws_access_key_id')
        aws_secret_access_key = config.get('AWS_S3', 'aws_secret_access_key')
        
        return region, aws_access_key_id, aws_secret_access_key

    def __check_controle_file(self, assunto):
        print(f'Verificando existencia de arquivos de controle de {assunto}')
        region, aws_access_key_id, aws_secret_access_key = self.__get_credentials()

        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id, 
            aws_secret_access_key=aws_secret_access_key,
            region_name=region
        )
        
        controle_path = f'0001_raw/tb_pagamentos/controle/'

        print(f'Path Controle: {controle_path}')

        try:
            response = s3_client.list_objects_v2(Bucket='lake-project-', Prefix = controle_path)

            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('.ctl'):
                        print(f'File found: {obj['Key'].split('/')[-1]}')
                        return obj['Key'].split('/')[-1]
            
            print('No .ctl file found in the specified folder.')
            return False

        except Exception as e:
            print(f'An error occurred: {e}')
            return False
    
    def __get_emr_client(self):
        region, aws_access_key_id, aws_secret_access_key = self.__get_credentials()

        return boto3.client('emr', region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    def __create_emr_cluster(self, assunto):
        print(f'Criando cluster emr')
        cluster_name = f'0001-book-{assunto}-lake-project'
        log_uri = 's3://lake-project-/0004_logs/stage_book_process'
        release_label = 'emr-6.0.0'
        master_instance_type = 'm5.xlarge'

        response = self.__emr_client.run_job_flow(
            Name = cluster_name,
            LogUri = log_uri,
            ReleaseLabel = release_label,
            Instances = {
                    'InstanceGroups': [
                {
                    'InstanceRole': 'MASTER',
                    'InstanceType': master_instance_type,
                    'InstanceCount': 1,
                },
            ]},
            Applications = [{'Name': 'Spark'}],
            ServiceRole = 'EMR_DefaultRole',
            JobFlowRole = 'EMR_EC2_DefaultRole'
        )

        print(f'Criado Cluster ID: {response['JobFlowId']}')

        cluster_id = response['JobFlowId']

        return cluster_id
    
    def __add_step_job(self, assunto, controle_file, cluster_id):
        print('Adicionando step job')
        step_name = f'process_lake_{assunto}'
        script_path = f's3://lake-project-/0005_scripts/stage_book/{assunto}/dedup_stage_book.py'
      
        print(script_path)
        print(controle_file)

        step = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--deploy-mode','client',script_path, controle_file],
            },
        }

        response = self.__emr_client.add_job_flow_steps(JobFlowId=cluster_id, Steps = [step])

        step_job_id = response['StepIds'][0]

        return step_job_id
    
    def __wait_step_job(self, step_id, cluster_id):
        while True:
            response = self.__emr_client.describe_step(ClusterId = cluster_id, StepId = step_id)
            state = response['Step']['Status']['State']

            if state in ['PENDING', 'RUNNING']:
                print(f'Executando step job...Estado: {state}')
                time.sleep(30)
            elif state == 'COMPLETED':
                print(f'Execução do step job finalizada...Estado: {state}')
                break
            else:
                raise Exception(f'O Step Job falhou com estado: {state}')

    def __terminate_emr_cluster(self, cluster_id):
        self.__emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
    
    def start(self, assunto):
        controle_file = self.__check_controle_file(assunto)

        if controle_file == False:
            raise Exception(f'Arquivo de Controle não encontrado para assunto {assunto}')

        cluster_id = self.__create_emr_cluster(assunto)

        step_id = self.__add_step_job(assunto, controle_file, cluster_id)

        self.__wait_step_job(step_id, cluster_id)

        self.__terminate_emr_cluster(cluster_id)