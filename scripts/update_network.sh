#!/bin/bash
docker-compose down && docker-compose -p $USER down
echo "Please shut down all remaining containers before continuing"
read -n 1 -p "Press any button to continue" -s && echo
docker network rm bpbarberini_default
docker network rm bp-barberini_default
docker network create database_network