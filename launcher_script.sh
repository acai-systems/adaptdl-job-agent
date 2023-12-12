#!/bin/bash
SDK_TAG=n_tag_0.5

echo '=================System Initializing================='
echo "-- installing git --"
apt-get update && apt-get install -y git unzip wget && apt-get install -y docker.io && pip install -r adaptdl_agent_requirements.txt
echo "-- installing dependencies --"
echo "--- using SDK version $SDK_TAG ---"
wget https://github.com/acai-systems/acaisdk/archive/refs/tags/$SDK_TAG.zip && unzip $SDK_TAG.zip -d acaisdk
cd acaisdk/acaisdk-$SDK_TAG && python3 -m pip install . && cd ../..
python3 -m pip install redis
git clone --single-branch --branch dev https://github.com/acai-systems/job-agent.git
cat acaisdk/acaisdk-$SDK_TAG/acaisdk/configs.ini
echo '===============Your Stuff Starts Here================='
python create_adaptdl_job.py
