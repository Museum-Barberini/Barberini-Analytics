#!/bin/bash
set -e

sudo docker network create barberini_analytics_database_network
sudo usermod -aG docker "$USER"
echo "Please reboot for user group changes to take effect"
while true; do
    read -r -p "Reboot now (y/n)? " yn
    case $yn in
        y|Y ) sudo reboot; break;;
        n|N ) exit;;
        * ) echo "Please answer y (for yes) or n (for no).";;
    esac
done
# TODO: Would it be sufficient to just restart the docker daemon?
