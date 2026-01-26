# Parquet Summary

## Development

### Installing the ``arrow`` and ``parquet`` parquet 

Here is the [official guide](https://arrow.apache.org/install/), for those who trust me (or don't want to read 
the details) here is how I installed the libraries:


```shell
sudo apt update
sudo apt install -y -V ca-certificates lsb-release wget
wget https://packages.apache.org/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt update
sudo apt install -y -V libarrow-dev libparquet-dev
```

This has been tested on Ubuntu 24.04.
