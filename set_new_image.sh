echo "getting new date"
mydate=$(date '+%Y%m%d%H%M%S')
echo $mydate

echo "building new image"
docker build -t docker.pdl.cmu.edu/acai/adaptdljobagent:$mydate .

echo "pushing new image"
docker push docker.pdl.cmu.edu/acai/adaptdljobagent:$mydate

echo "setting new image"
kubectl set image deployment/adaptdldeployment adaptdljobagent=docker.pdl.cmu.edu/acai/adaptdljobagent:$mydate -n acai
