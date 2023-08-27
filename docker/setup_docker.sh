docker build -t wqp docker
docker run --name wqp -v $(pwd):/usr/local/processing/dqm-pipeline -it wqp
