


#!/bin/bash

# Set the target image name
IMAGE_NAME="bellout/fastapi"

# Find the container ID of the running container with the given image name
CONTAINER_ID=$(docker ps --filter "ancestor=$IMAGE_NAME" --format "{{.ID}}")

# Check if the container ID is not empty
if [ ! -z "$CONTAINER_ID" ]; then
  echo "Stopping container with ID: $CONTAINER_ID"
  # Stop the container
  docker stop "$CONTAINER_ID"

  echo "Removing container with ID: $CONTAINER_ID"
  # Remove the container
  docker rm "$CONTAINER_ID"
else
  echo "No running container found with image: $IMAGE_NAME"
fi

# Check if the image exists
IMAGE_ID=$(docker images --filter "reference=$IMAGE_NAME" --format "{{.ID}}")

# If the image ID is not empty, remove the image
if [ ! -z "$IMAGE_ID" ]; then
  echo "Removing image: $IMAGE_NAME"
  docker rmi -f "$IMAGE_NAME"
else
  echo "Image not found: $IMAGE_NAME"
fi


#! / bin / bash
# docker ps -a | grep 'bellout/fastapi*' | awk '{print $1}' | xargs docker stop
# docker ps -a | grep 'bellout/fastapi*' | awk '{print $1}' | xargs docker rm
# docker rmi -f $(docker image ls "bellout/fastapi*")

# docker stop $(docker ps -a -q --filter status=running --format="{{.ID}}") 
# docker rm -f $(docker ps -a -q --filter name=registry.digitalocean.com/microteksregistry/microteksapi* --format="{{.ID}}") 
# docker rm -f $(docker ps -a -q --filter name=microteksapi* --format="{{.ID}}") 
# docker rmi -f $ (docker image ls "microteksapi*")
# docker rmi -f $ (docker image ls "registry.digitalocean.com/microteksregistry/microteksapi*")
