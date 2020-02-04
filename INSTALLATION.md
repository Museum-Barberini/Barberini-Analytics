# Installation

## Requirements

- UNIX system (preferred Ubuntu; does not work well with WSL)

Please note that this instructions are optimized for Ubuntu, amd64. If you use a different configuration, you may need to adapt the toolchain installation (see `install_toolchain.sh`).

## Let’s go!

1. Clone the repository using git

   ```bash
   git clone https://gitlab.hpi.de/bp-barberini/bp-barberini.git
   ```

2. Copy the `secrets` folders (not available on the internet) into `/etc`

3. Set up the toolchain:

   ```bash
   sudo scripts/install_toolchain.sh
   ```

4. Set up docker and db:

   ```bash
   ./scripts/update_network.sh
   ```
