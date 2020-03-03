# BP Barberini

![coverage](https://gitlab.hpi.com/bp-barberini/bp-barberini/badges/master/coverage.svg)

### Run luigi

At the moment we are using a setup with two docker containers. One container is used to 
run the luigi pipeline (the luigi container), one container is used for a postgres database
(the postgres container). The two containers are connected with a docker network.

1. Make sure you have [docker](https://docs.docker.com/v17.09/engine/installation/) installed.
2. Pull the base images (ubuntu and postgres): `[sudo] make pull`
3. Build the docker image for the luigi container (this will take a while): `[sudo] make build-luigi`
4. Startup the two docker containers and connect them with a docker network: `[sudo] make startup`
5. Get a terminal in the luigi container: `[sudo] make connect`
6. From inside the luigi container
    1. Start the scheduling server: `make luigi-scheduler`
    2. Run the luigi pipeline: `make luigi`
7. Take a look at the visualization: `http://localhost:8082`
8. When you are done kill the two containers and remove the network: `[sudo] make shutdown`

