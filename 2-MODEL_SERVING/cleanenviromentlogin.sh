
#! / bin / bash
docker stop $(docker ps -a -q --filter status=running --format="{{.ID}}") 


# Define target image repositories
IMAGE_REPO1="registry.digitalocean.com/microteksregistry/microteksapi"
IMAGE_REPO2="microteksapi"

# Function to handle image removal
remove_images() {
  local IMAGE_REPO="$1"

  # Find all images from the repository (ignoring the tag)
  IMAGES=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "^$IMAGE_REPO")

  if [ -z "$IMAGES" ]; then
    echo "No images found for repository: $IMAGE_REPO"
  else
    # Loop through each image
    for IMAGE in $IMAGES; do
      # Check for running containers using this image
      CONTAINER_ID=$(docker ps -a --filter "ancestor=$IMAGE" --format "{{.ID}}")
      
      if [ ! -z "$CONTAINER_ID" ]; then
        echo "Stopping container with ID: $CONTAINER_ID"
        # Stop the container
        docker stop "$CONTAINER_ID"

        echo "Removing container with ID: $CONTAINER_ID"
        # Remove the container
        docker rm "$CONTAINER_ID"
      else
        echo "No running container found with image: $IMAGE"
      fi

      # Now, remove the image
      echo "Removing image: $IMAGE"
      docker rmi -f "$IMAGE"
    done
  fi
}

# Attempt to remove images for both repositories
remove_images "$IMAGE_REPO1"
remove_images "$IMAGE_REPO2"
