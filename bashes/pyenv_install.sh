#!/bin/bash
#
# Pyenv Installer Script
#
# This script automates the installation of Pyenv and the latest Python versions
# on a Debian-based system or macOS. It installs necessary dependencies, sets up Pyenv,
# restarts the terminal, and installs the latest Python versions concurrently.
#
# Note: Ensure you have sudo privileges to install system dependencies.
#
# For more information on Pyenv, visit https://github.com/pyenv/pyenv
#
# Created by Henrique Lobato
# Date: Jan 11, 2024
#

# Detect operating system
if [[ "$(uname)" == "Darwin" ]]; then
    # macOS
    echo "Detected macOS. Installing dependencies using Homebrew..."
    brew install openssl readline sqlite3 xz zlib
else
    # Debian-based systems
    echo "Detected Debian-based system. Installing dependencies using apt-get..."
    sudo apt-get install -y make build-essential libssl-dev zlib1g-dev \
    libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev \
    libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python-openssl
fi

# Install pyenv
echo "Installing pyenv..."
curl -fsSL https://pyenv.run | bash

# Restart the terminal
echo "Restarting the terminal..."
exec "$SHELL" &

# Install dependencies and Python versions concurrently
(
    # Install dependencies in the background
    for version in "${latest_versions[@]}"; do
        if [ -n "$version" ]; then
            echo "Installing Python $version..."
            pyenv install -v 3.6.10 &
        fi
    done

    # Wait for background processes to finish
    wait

    # Display installed Python versions
    echo "Installed Python versions:"
    pyenv versions
) &