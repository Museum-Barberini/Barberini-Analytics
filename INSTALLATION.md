# Installation

## Requirements

- UNIX system (prefered Ubuntu; does not work well with WSL)

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
