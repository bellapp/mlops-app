
#! / bin / bash
docker stop $(docker ps -a -q --filter status=running --format="{{.ID}}") 
docker rm -f $(docker ps -a -q --filter name=registry.digitalocean.com/microteksregistry/microteks_data_api* --format="{{.ID}}") 
docker rm -f $(docker ps -a -q --filter name=microteks_data_api* --format="{{.ID}}") 
docker rmi -f $ (docker image ls "microteks_data_api*")
docker rmi -f $ (docker image ls "registry.digitalocean.com/microteksregistry/microteks_data_api*")
