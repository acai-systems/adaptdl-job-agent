#!/usr/bin/env python3

import argparse
import copy
import itertools
import os
import subprocess
import time
import yaml
import zipfile

from kubernetes import client, config, watch
from flask import Flask, request, jsonify
import requests
import docker
print(os.getcwd())
print(os.listdir())
from acaisdk.fileset import FileSet
from acaisdk.file import File


app = Flask(__name__)

def build_image(model, repository):
    with cd(os.environ['DATA_LAKE_MOUNT_PATH']):
        model_path =  os.path.join(model)
        print(os.getcwd(), flush=True)
        print(os.listdir(), flush=True)
        with open(os.path.join(model_path, "adaptdljob.yaml")) as f:
            template = yaml.load(f, Loader=yaml.Loader)
        image = repository + ":" + model
        print(f"Image is {image}")
        original_directory = os.getcwd()
        os.chdir(os.path.join(model_path))
        subprocess.check_call(["docker", "build", "-t", image, "."])
        subprocess.check_call(["docker", "push", image])
        os.chdir(original_directory)
        repodigest = subprocess.check_output(
                ["docker", "image", "inspect", image, "--format={{index .RepoDigests 0}}"])
        repodigest = repodigest.decode().strip()
        template["spec"]["template"]["spec"]["containers"][0]["image"] = repodigest
    return template


def cache_image(model, template):
    # Cache job images on all nodes in the cluster.
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

    daemonset["spec"]["template"]["spec"]["containers"].append({
        "name": model,
        "image": template["spec"]["template"]["spec"]["containers"][0]["image"],
        "command": ["sleep", "1000000000"],
    })
    apps_api = client.AppsV1Api()
    apps_api.create_namespaced_daemon_set("acai", daemonset)

    while True:
        # Wait for DaemonSet to be ready.
        obj = apps_api.read_namespaced_daemon_set("images", "acai")
        ready = obj.status.number_ready
        total = obj.status.desired_number_scheduled
        print("caching images on all nodes: {}/{}".format(ready, total))    
        if total > 0 and ready >= total: break
        time.sleep(10)

class cd:
    def __init__(self, newPath):
        self.newPath = newPath

    def __enter__(self):
        if not os.path.exists(self.newPath):
            os.mkdir(self.newPath)
        self.oldPath = os.getcwd()
        os.chdir(self.newPath)

    def __exit__(self, etype, value, traceback):
        os.chdir(self.oldPath)

'''
job_setting = {
    "v_cpu": "0.4",
    "memory": "512Mi",
    "gpu": "0",
    "command": "mkdir -p ./my_output/ && (cat Shakespeare/* | python3 wordcount.py ./my_output/)",
    "container_image": "pytorch/pytorch",
    'input_file_set': 'shakespeare.works',
    'output_path': './my_output/',
    'code': '/wordcount.zip',
    'description': 'count some words from Shakespeare works',
    'name': 'my_acai_job_terraform_test'
}
'''

def download_files(jobID):
    jobDetails = requests.get(f"http://job-registry-service.acai.svc.cluster.local:80/job_registry/job?job_id={jobID}").json()
    os.environ['JOB_ID'] = jobID
    os.environ['PROJECT_ID'] = str(jobDetails['project_id'])
    os.environ['USER_ID'] = str(jobDetails['user_id']) 
    os.environ['INPUT_FILE_SET'] = jobDetails['input_file_set']
    os.environ['CODE'] = jobDetails['code']
    os.environ['DATA_LAKE_MOUNT_PATH'] = 'acai'
    os.environ['CEPH_CACHE_MOUNT_PATH'] = 'cache'
    os.environ['REDIS_HOST'] = 'redis-service.acai.svc.cluster.local'
    os.environ['REDIS_PWD'] = '123abc'
    os.environ['REDIS_PORT'] = '6379'
    token = requests.get(f"http://phoebe-mgmt.pdl.local.cmu.edu:30379/credential/resolve_user_id?id={os.environ['USER_ID']}").json()['token']
    os.environ['ACAI_TOKEN'] = token
    with cd(os.environ['DATA_LAKE_MOUNT_PATH']):
        # FileSet.download_file_set(os.environ['INPUT_FILE_SET'], ".", force=True)
        code_path = "./" + jobDetails['code']
        File.download({jobDetails['code']: code_path})
        with zipfile.ZipFile(code_path, "r") as ref:
            ref.extractall()
        print("After extracting code")
        print(os.listdir())
        print(os.listdir('helloworld/'))


def create_job(jobID, modelName):
    repository = "docker.pdl.cmu.edu/pollux"
    download_files(jobID)
    config.load_incluster_config()
    print(f"Clearing images in kubectl")
    image_to_build = modelName
    print(f"Building images: {image_to_build}", flush=True)
    template = build_image(image_to_build, repository)
    print(template)
    cache_image(image_to_build, template)
    objs_api = client.CustomObjectsApi()
    namespace = "acai"
    obj_args = ("adaptdl.petuum.com", "v1", namespace, "adaptdljobs")
    application = modelName
    num_replicas = 1
    batch_size = 320
    name = modelName

    job = copy.deepcopy(template)
    job["metadata"].pop("generateName")
    job["metadata"]["name"] = name
    job["spec"].update({
        "application": application,
        "targetNumReplicas": num_replicas,
        "targetBatchSize": batch_size,
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
    print(yaml.dump(job))
    objs_api.create_namespaced_custom_object(*obj_args, job)


@app.route('/create_job', methods=['POST'])
def create_job_route():
    print(f'Full Request Data: {request.data}')
    print(f'Headers: {request.headers}')
    jobID = request.json.get('job_id', '')
    modelName = request.json.get("model_name", '')
    create_job(jobID, modelName)
    return jsonify({'message': 'Job created successfully'})

if __name__ == "__main__":
    # Run the Flask application
    docker_client = docker.from_env()
    subprocess.check_call(["docker", "login", "-u", "<username>", "-p", "<password>", "docker.pdl.cmu.edu"])
    print("Login successful")   
    app.run(host='0.0.0.0', port=37654)



