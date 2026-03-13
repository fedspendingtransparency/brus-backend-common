
## Local Development Environment Setup

This setup should provide containers to develop and test common scripts, models, etc. shared between the [Data Broker](https://github.com/fedspendingtransparency/data-act-broker-backend) and [USAspending](https://github.com/fedspendingtransparency/data-act-broker-backend) applications.

Ensure the following dependencies are installed and working prior to continuing:

### Requirements
- [`docker`](https://docs.docker.com/install/) which will handle the other application dependencies.
- [`docker compose`](https://docs.docker.com/compose/)
- `bash` or another Unix Shell equivalent
    - Bash is available on Windows as [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10)
- [`git`](https://git-scm.com/downloads)

_**If not using Docker, you'll need to install app components on your machine:**_
> _Using Docker is recommended since it provides a clean environment. Setting up your own local environment requires some technical abilities and experience with modern software tools._

- Command line package manager
    - Windows' WSL bash uses `apt`
    - MacOS users can use [`Homebrew`](https://brew.sh/)
    - Linux users already know their package manager (`yum`, `apt`, `pacman`, etc.)
- `Python` version (see version tagged in [README.md](README.md)). [Installation guide]((https://docs.python-guide.org/starting/installation/#python-3-installation-guides))
  - Highly recommended to use a virtual environment. There are various tools and associated instructions depending on preferences
  - See [Required Python Libraries](#required-python-libraries) for an example using `uv`
- [`uv`](https://github.com/astral-sh/uv) python package/project manager

### Cloning the Repository
Now, navigate to the base file directory where you will store the repository

```shell
cd [repo directory]
git clone https://github.com/fedspendingtransparency/brus-backend-common.git
cd brus-backend-common
```

### Environment Variables

Choose an option between `.env` and `.envrc` that best fits your preferred workflow. Pay close attention to the values in these environment variables as usage of `localhost` vs a container's name differ between local setups.

**Note: Explanations for the environment variables will be in [config.py](brus_backend_common/config.py), since it includes additional derived values in the config, but make sure to only modify your .env file to prevent confusion.**

#### Create Your `.env` File (recommended)
Copy the template `.env` file with local runtime environment variables defined. Change as needed for your environment. _This file is git-ignored and will not be committed by git if changed._

```shell
cp .env.template .env
```

A `.env` file is a common way to manage environment variables in a declarative file. Certain tools, like `docker compose`, will read and honor these variables.

#### Create Your `.envrc` File
_[direnv](https://direnv.net/) is a shell extension that automatically runs shell commands in a `.envrc` file (commonly env var `export` commands) when entering or exiting a folder with that file_

Create a `.envrc` file in the repo root, which will be ignored by git. Change credentials and ports as-needed for your local dev environment.

```shell
export [config name]=[config value]
```

If `direnv` does not pick this up after saving the file, type

```shell
direnv allow
```
_Alternatively, you could skip using `direnv` and just export these variables in your shell environment._

**Just make sure your env vars declared in the shell and in `.env` match for a consistent experience inside and outside of Docker**

### Build `brus-backend-common` Docker Image
_This image is used as the basis for running application components and running containerized setup services._

If you have any existing builds/containers, this will clear out for a fresh new install
```shell
docker-compose --profile '*' down -v
```

Build,
```shell
docker-compose --profile '*' build
```

And run in the background,
```shell
docker-compose --profile '*' up -d
```

You can update environment variables in `.env` (buckets, local paths) and they will be mounted and used when you run this.

_:bangbang: Re-run this command if any python package dependencies change (in `pyproject.toml`/`uv.lock`), since they are baked into the docker image at build-time._

Since changes to mounted files on containers cannot affect the source files on the host machine, it's recommended to: 
* install `uv` locally (depending on your host machine)
* remove the containers (see above)
* run `uv sync` in this directory to update the uv.lock file
* and `rm -r .venv` to clear out the extra directories generated from the sync.
* reinstall the containers

### Debugging

Tests will also automatically run on the `brus-backend-common-ci` container. These will essentially be the same PR checks so make sure to address any/all issues that result from your changes. Simply restart the container to rerun them.

You can also manually hop on the `brus-backend-common` container and run the checks individually. These include:
* `mypy ./` (for type checking, not required)
* `black .` (for styling)
* `flake8` (for additional styling and pythonic checks)
* `pytest .` (or whichever specific test you're running)

When writing tests, be sure to utilize all the available pytest fixtures located in [conftest.py](brus_backend_common/tests/conftest.py) and [conftest_spark.py](brus_backend_common/tests/conftest_spark.py).

### Logging

The majority of the logging configuration can be found in [logging.py](brus_backend_common/logging.py).

Additionally, there are additional verbose spark logs that have been intentionally silenced for local development using spark configurations and Neo4j properties files via [spark_helpers.py](brus_backend_common/tests/conftest_spark.py).
