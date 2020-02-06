# Installation

## Requirements

- UNIX system (preferred Ubuntu; does not work well with WSL)

Please note that this instructions are optimized for Ubuntu, amd64. If you use a different configuration, you may need to adapt the toolchain installation (see `install_toolchain.sh`).

## Let’s go!

1. Clone the repository using git

   ```bash
   git clone https://gitlab.hpi.de/bp-barberini/bp-barberini.git
   cd bp-barberini
   chmod +x scripts/*.sh
   ```
   
   - For best convenience, clone it into `/root/bp-barperini`.

2. Copy the `secrets` folders (not available on the internet) into `/etc`

3. Set up the toolchain:

   ```bash
   sudo scripts/install_toolchain.sh
   ```

4. Set up docker and db:

   ```bash
   ./scripts/setup_docker.sh
   ./scripts/update_network.sh
   ```

## Schedule regular DB updates

Run `scripts/setup_cron.sh`. If you installed Awesome Barberini Tool in a different folder than `/root/bp-barberini`, you may want to adapt the paths in `scripts/.crontab` before. If no crontab exists before, create it using `crontab -e`.
