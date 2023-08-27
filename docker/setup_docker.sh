docker build -t dqm docker
docker run --name dqm -v $(pwd):/usr/local/processing/dqm-pipeline -it dqm
