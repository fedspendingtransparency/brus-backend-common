# BRUS Backend Common

[![python: 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/) [![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/python/black) [![Pull Request Checks](https://github.com/fedspendingtransparency/brus-backend-common/actions/workflows/pull-request-checks.yaml/badge.svg)](https://github.com/fedspendingtransparency/brus-backend-common/actions/workflows/pull-request-checks.yaml)

This repository will act as place to share libraries, models, helpers, and scripts between the [Data Broker](https://github.com/fedspendingtransparency/data-act-broker-backend) and [USAspending](https://github.com/fedspendingtransparency/data-act-broker-backend) applications.

## Structure

This repo contains:

* **[helpers](brus_backend_common/helpers "Helpers"):** common ad-hoc functions independent shared among the systems.
* **[libraries](brus_backend_common/libraries "Libraries"):** common libraries shared among the systems.
* **[models](brus_backend_common/models "Models"):** data models shared among the systems.
* **[scripts](brus_backend_common/scripts "Scripts"):** scripts shared among the systems.
* **[tests](brus_backend_common/tests "Tests"):** unit and integration tests for all of the above.
* **[config](brus_backend_common/config.py "Config"):** shared config values for all of the above.
* **[lookups](brus_backend_common/lookups.py "Lookups"):** shared lookup values for all of the above.
* **[logging](brus_backend_common/logging.py "Logging"):** shared logging settings, functions for all of the above.

### Contributing and Installation

If you want to contribute with a local installation, follow the instructions on our [contributing guide](doc/CONTRIBUTING.md) and [install guide](doc/INSTALL.md "INSTALL.md").
