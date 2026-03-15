#!/bin/bash
# Startup script for OmnipaxosKV GCP instances. This script is executed during instance creation.
# For debugging, SSH into the instance and run `sudo journalctl -u google-startup-scripts.service`.

# Get OS login user and image path from instance metadata
OSLOGIN_USER=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/oslogin_user)
DOCKER_IMAGE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/docker_image)

# Ensure OS login user is setup
useradd -m $OSLOGIN_USER
mkdir -p /home/$OSLOGIN_USER
chown $OSLOGIN_USER:$OSLOGIN_USER /home/$OSLOGIN_USER

# Configure Docker credentials for the user
sudo -u $OSLOGIN_USER docker-credential-gcr configure-docker --registries=gcr.io
sudo -u $OSLOGIN_USER echo "https://gcr.io" | docker-credential-gcr get
sudo groupadd docker
sudo usermod -aG docker $OSLOGIN_USER

# Pull the container as user
sudo -u $OSLOGIN_USER docker pull $DOCKER_IMAGE

# Fetch the run_container.sh script from instance metadata
curl -f -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/run_container_script" -o home/$OSLOGIN_USER/run_container.sh
chmod +x /home/$OSLOGIN_USER/run_container.sh
