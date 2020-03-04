# Awesome Barberini Tool

A suite of data analytics tools to create an awesome dashboard for the Museum Barberini, Potsdam, in order to help them analyze and assess customer, advertising, and social media data!  

## Installation

### Requirements

- UNIX system (preferred Ubuntu; does not work well with WSL)

Please note that these instructions are optimized for Ubuntu, amd64. If you use a different configuration, you may need to adapt the toolchain installation (see `install_toolchain.sh`).

### Actual installation

1. Clone the repository using git

   ```bash
   git clone https://gitlab.hpi.de/bp-barberini/bp-barberini.git
   cd bp-barberini
   chmod +x scripts/*.sh
   ```
   
   - For best convenience, clone it into `/root/bp-barberini`.

2. Copy the `secrets` folders (not available on the internet) into `/etc`

3. Set up the toolchain. See `scripts/install_toolchain.sh` how to do this. If you use ubuntu/amd64, you can run the script directly. Use `sudo`!

4. Set up docker network and add the current user to the `docker` user group.

   ```bash
   ./scripts/setup_docker.sh
   ```

### Schedule regular DB updates

Run `sudo scripts/setup_cron.sh`. If you cloned the repository in a different folder than `/root/bp-barberini`, you may want to adapt the paths in `scripts/.crontab` first. If no crontab exists before, create it using `crontab -e`.


## Usage

### Controlling the pipeline

#### Open the luigi webinterface

```bash
 make docker-do do='make luigi-scheduler'
```

This will also start a webserver on http://localhost:8082 where you can trace all running tasks.

#### Running the pipeline manually

```bash
 make docker-do do='make luigi'
```

#### Accessing the docker containers

Have a look at our beautiful `Makefile`! To access the luigi docker, do:

```bash
make startup connect
```

Close the session by executing:

```bash
make shutdown
```

## Credits

**Authors:** Laura Holz, Selina Reinhard, Leon Schmidt, Georg Tennigkeit, Christoph Thiede, Tom Wollnik (bachelor project BPFN1 @ HPI).
**Organizations:** [Hasso Plattner Institute, Potsdam](https://hpi.de/en); [Museum Barberini](https://www.museum-barberini.com/en/); Hasso Plattner Foundation.
