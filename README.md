# Snowflake DAG Deploy

This is a simple example of how to deploy a DAG to Snowflake using the Snowflake Task API.

The [Snowflake task API](https://docs.snowflake.com/en/developer-guide/snowflake-python-api/reference/latest/_autosummary/snowflake.core.task.dagv1) seems to be able to manage the life cycle of DAG and its tasks, however, you have to custom code the deployment. So I was curious what it would take to make things a bit more declarative and configuration driven.

## Prerequisites

You will need a `~/.snowflake/config.toml` file configured. See [Configuring Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/connecting/configure-cli) for more information.

## Usage

Dags are defined in the `dags.yml` file. `deploy.py` reads this file and creates the DAG and tasks in Snowflake.
