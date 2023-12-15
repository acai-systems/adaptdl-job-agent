# adaptdl-job-agent
AdaptDL Job Agent Service: A microservice that acts as an intermediary between the ACAI Systems Platform and the AdaptDL scheduler, to support submitting Deep Learning jobs from the ACAI Interface. 

Files:
1. create_adaptdl_job.py: A Flask app that provides an async `/create_job` endpoint which the ACAI Platform microservices can use to submit Deep Learning Jobs. See the code for API specifications.
2. set_new_image.sh: Script to automatically take the latest code changes in create_adaptdl_job.py and push a new image of the AdaptDL Job Agent service to the ACAI Image Registry.
3. launcher_script.sh: Script that runs when a new image of the pod gets pulled into the pod during pod creation - an optimization to reduce the size of the image.
4. Dockerfile: Image build specifications
5. pvc.cephfs.yaml: PVC used by the AdaptDL Job Agent to share code/datasets to AdaptDL jobs and to pull checkpointed model weights from AdaptDL job. 

Setup Steps:
1. While installing the ACAI Systems helm chart, the AdaptDL Job Agent will be deployed as a pod.
2. For debugging the AdaptDL job agent, one can run `./set_new_image.sh` script which will automatically build a new Docker image based on the latest changes in create_adaptdl_job.py and push it to the ACAI Image Registry, after which it will set the image of the existing AdaptDL Job Agent deployment to the new image, resulting in the AdaptDL Job Agent service to be restarted.
