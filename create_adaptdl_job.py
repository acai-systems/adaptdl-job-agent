#!/usr/bin/env python3
import argparse
import copy
import itertools
import os
import subprocess
import time
import yaml
import zipfile
import tarfile
from kubernetes import client, config, watch
from flask import Flask, request, jsonify
import requests
import docker
from acaisdk.fileset import FileSet
from acaisdk.file import File
from threading import Thread

import sys
from logging.config import dictConfig

import json
import redis

import asyncio
from kubernetes import client, config
import queue

app = Flask(__name__)



class Publisher:
    def __init__(self, job_id, user_id, host, port, pwd=None):
        self.__job_id = job_id
        self.__user_id = user_id
        self.__r = redis.Redis(host=host, port=port, password=pwd)

    def progress(self, message):
        self.__r.publish(
            "job_progress", "{}:{}:{}".format(self.__job_id, self.__user_id, message)
        )


job_queue = queue.Queue()

publishers = {}
job_name_id_map = {}


def trigger_job(job_id, name, application, target_batch_size, target_number_replicas):
    repository = "docker.pdl.cmu.edu/pollux"
    download_files(job_id)
    config.load_incluster_config()
    print(f"Clearing images in kubectl")
    image_to_build = application
    print(f"Building images: {image_to_build}", flush=True)
    template = build_image(image_to_build, repository)
    publishers[job_id].progress("Building image")
    print(template)
    objs_api = client.CustomObjectsApi()
    namespace = "acai"
    obj_args = ("adaptdl.petuum.com", "v1", namespace, "adaptdljobs")

    job = copy.deepcopy(template)
    job["metadata"].pop("generateName")
    job["metadata"]["name"] = name
    job["spec"].update({
        "application": application,
        "targetNumReplicas": target_number_replicas,
        "targetBatchSize": target_batch_size,
    })
    volumes = job["spec"]["template"]["spec"].setdefault("volumes", [])
    volumes.append({
        "name": "pollux",
        "persistentVolumeClaim": { "claimName": "pollux" },
    })

    mounts = job["spec"]["template"]["spec"]["containers"][0].setdefault("volumeMounts", [])
    mounts.append({
        "name": "pollux",
        "mountPath": "/pollux/checkpoint",
        "subPath": "pollux/checkpoint/" + name,
    })
    mounts.append({
        "name": "pollux",
        "mountPath": "/pollux/checkpoint-job",
        "subPath": "pollux/checkpoint-job/" + name,
    })

    mounts.append({
        "name": "pollux",
        "mountPath": "/pollux/tensorboard",
        "subPath": "pollux/tensorboard/" + name,
    })

    mounts.append({"name": "pollux", "mountPath": "/pollux/"})
    env = job["spec"]["template"]["spec"]["containers"][0].setdefault("env", [])
    env.append({"name": "ADAPTDL_CHECKPOINT_PATH", "value": "/pollux/checkpoint"})
    env.append({"name": "ADAPTDL_TENSORBOARD_LOGDIR", "value": "/pollux/tensorboard"})
    env.append({"name": "APPLICATION", "value": application})
    objs_api.create_namespaced_custom_object(*obj_args, job)
    publishers[job_id].progress("Published")

def process_queue():
    print("Starting process queue")
    global job_queue
    global publishers
    print(f"create_job pid: {os.getpid()}")
    print(f"job_queue in process_queue {job_queue}")
    while True:
        print(f"job_queue size: {job_queue.qsize()}")
        try:
            job_item = job_queue.get_nowait()
        except:
            print("No job available", flush=True)
            time.sleep(5)
            continue
        print(f"process_queue job_item in process_queue is {job_item}")
        if job_item is not None:
            job_id = job_item['job_id']
            name = job_item['name']
            application = job_item['application']
            target_batch_size = job_item['target_batch_size']
            target_number_replicas = job_item['target_number_replicas']
            print(f"Processing job: {job_id} with name: {name}, application: {application}, target_batch_size: {target_batch_size}, target_number_replicas: {target_number_replicas}")
            trigger_job(job_id, name, application, target_batch_size, target_number_replicas) 
            publishers[job_id].progress("Dequeued")

            




def monitor_adaptdl_jobs():
    global job_name_id_map
    global publishers
    while True:
        w = watch.Watch()
        stream = w.stream(
            client.CustomObjectsApi().list_namespaced_custom_object,
            "adaptdl.petuum.com",
            'v1',
            "acai",
            'adaptdljobs'
        )
        seen_jobs = set()
        for event in stream:
            if "raw_object" not in event:
                continue
            if "metadata" not in event["raw_object"]:
                continue
            if "status" not in event["raw_object"]:
                continue
            if "name" not in event["raw_object"]["metadata"]:
                continue
            if "phase" not in event["raw_object"]["status"]:
                continue
            adaptdl_job_name = event['raw_object']['metadata']['name']
            job_status = event['raw_object']['status']['phase']
            print(f"Monitoring adaptdl_job_name: {adaptdl_job_name}, job_status: {job_status}")
            if "adaptdl_job_name" not in job_name_id_map:
                continue
            if job_name_id_map[adaptdl_job_name] not in publishers:
                continue
            publishers[job_name_id_map[adaptdl_job_name]].progress(job_status)
            seen_jobs.add(adaptdl_job_name)
        for job_name in job_name_id_map:
            if job_name not in seen_jobs:
                print(f"AdaptDL job deleted {job_name}, job_status: Completed")
                publishers[job_name_id_map[adaptdl_job_name]].progress("Completed")
        time.sleep(5)


def get_daemonset_definition(namespace, daemonset_name):
    apps_api = client.AppsV1Api()
    try:
        daemonset = apps_api.read_namespaced_daemon_set(daemonset_name, namespace)
        return daemonset, True
    except client.rest.ApiException as e:
        daemonset = {
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "metadata": {"name": "images"},
            "spec": {
                "selector": {"matchLabels": {"name": "images"}},
                "template": {
                    "metadata": {"labels": {"name": "images"}},
                    "spec": {
                        #"tolerations": [{"key": "experiment-reserved", "operator": "Equal", "value": "suhasj", "effect": "NoSchedule"}],
                        "containers": [],
                        "imagePullSecrets": [{"name": "regcred"}],
                    }
                }
            }
        }
        return daemonset, False

def build_image(model, repository):
    with cd(os.environ['POLLUX_MOUNT_PATH']):
        model_path =  os.path.join(model)
        with open(os.path.join(model_path, "adaptdljob.yaml")) as f:
            template = yaml.load(f, Loader=yaml.Loader)
        image = repository + ":" + model
        print(f"Image is {image}", flush=True)
        original_directory = os.getcwd()
        os.chdir(os.path.join(model_path))  
        print(f"model_path: {model_path}", flush=True)
        subprocess.check_call(["docker", "build", "-t", image, "."])
        subprocess.check_call(["docker", "push", image])
        os.chdir(original_directory)
        repodigest = subprocess.check_output(
                ["docker", "image", "inspect", image, "--format={{index .RepoDigests 0}}"])
        repodigest = repodigest.decode().strip()
        template["spec"]["template"]["spec"]["containers"][0]["image"] = repodigest
    return template



class cd:
    def __init__(self, newPath):
        self.newPath = newPath

    def __enter__(self):
        if not os.path.exists(self.newPath):
            print(f"{self.newPath} directory does not exist - creating")
            os.mkdir(self.newPath)
        else:
            print(f"{self.newPath} directory does exist - switching")
        self.oldPath = os.getcwd()
        os.chdir(self.newPath)
        print(os.listdir())

    def __exit__(self, etype, value, traceback):
        os.chdir(self.oldPath)

dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://sys.stdout',  # <-- Solution
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})

def download_files(job_id):
    job_details = requests.get(f"http://job-registry-service.acai.svc.cluster.local:80/job_registry/job?job_id={job_id}").json()
    if job_id in publishers:
        publishers[job_id].progress("Downloading files")
    print(job_details)
    os.environ['JOB_ID'] = job_id
    os.environ['PROJECT_ID'] = str(job_details['project_id'])
    os.environ['USER_ID'] = str(job_details['user_id']) 
    os.environ['INPUT_FILE_SET'] = job_details['input_file_set']
    os.environ['CODE'] = job_details['code']
    os.environ['DATA_TAR'] = job_details['data_tar']
    os.environ['DATA_LAKE_MOUNT_PATH'] = 'acai'
    os.environ['CEPH_CACHE_MOUNT_PATH'] = 'cache'
    os.environ['POLLUX_MOUNT_PATH'] = '/pollux'
    # token = requests.get(f"http://phoebe-mgmt.pdl.local.cmu.edu:30379/credential/resolve_user_id?id={os.environ['USER_ID']}").json()['token']
    # os.environ['ACAI_TOKEN'] = token
    # with cd(os.environ['DATA_LAKE_MOUNT_PATH']):
    #     code_path = "./" + jobDetails['code']
    #     print("Downloading code")
    #     start_time = time.time()
    #     File.download({jobDetails['code']: code_path})
    #     end_time = time.time()
    #     print(f"Time it took to download code: {end_time - start_time}")
    #     with zipfile.ZipFile(code_path, "r") as ref:
    #         ref.extractall()
    #     print("After extracting code")
    #     print(os.listdir())
        
    # with cd(os.environ['POLLUX_MOUNT_PATH']):
    #     tar_path = "./" + os.environ['DATA_TAR']
    #     print(f"Downloading input file set {os.environ['INPUT_FILE_SET']}")
    #     start_time = time.time()
    #     FileSet.download_file_set(os.environ['INPUT_FILE_SET'], ".", force=True)
    #     end_time = time.time()
    #     print(os.listdir())
    #     print(f"Time it took to download file set {end_time - start_time}")
    #     with tarfile.open(tar_path, 'r:gz') as tar:
    #         tar.extractall(path=".")
        

@app.route('/create_job', methods=['POST'])
def create_job():
    global job_queue
    global publishers
    global job_name_id_map
    print(f"create_job pid: {os.getpid()}")
    data = request.json
    job_id = data.get('job_id', '')
    name = request.json.get('name', '')
    application = request.json.get("application", '')
    target_batch_size = request.json.get("targetBatchSize", '')
    target_number_replicas = request.json.get("targetNumberReplicas", '')
    if (job_id is not None and name is not None and application is not None and target_batch_size is not None and target_number_replicas is not None):
        job_data = {'job_id': job_id, 'name': name, 'application': application, 'target_batch_size': target_batch_size, 'target_number_replicas': target_number_replicas}
        print("adding to queue")
        print(job_queue)
        job_queue.put(job_data)
        print(f"create_job job_queue size: {job_queue.qsize()}")
        if job_id not in publishers:
            publishers[job_id] = Publisher(
                job_id, "", host=os.environ['REDIS_HOST'], port=os.environ['REDIS_PORT'], pwd=os.environ['REDIS_PWD']
            )
            job_name_id_map[name] = job_id
        publishers[job_id].progress("Queued")         
        return jsonify({'message': 'Job added to queue successfully'}), 200
    else:
        return jsonify({'error': 'Job not provided in the request'}), 400



if __name__ == "__main__":
    # Run the Flask application
    docker_client = docker.from_env()
    config.load_incluster_config()
    subprocess.check_call(["docker", "login", "-u", "<username>", "-p", "<password>", "docker.pdl.cmu.edu"])
    print("Login successful")   
    agent_thread = Thread(target=process_queue)
    agent_thread.start()
    k8s_monitor_thread = Thread(target=monitor_adaptdl_jobs)
    k8s_monitor_thread.start()
    os.environ['REDIS_HOST'] = 'redis-service.acai.svc.cluster.local'
    os.environ['REDIS_PWD'] = '123abc'
    os.environ['REDIS_PORT'] = '6379'
    app.run(host='0.0.0.0', port=37654)

