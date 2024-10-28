---
title: "Onboarding Guide"
date: 2021-08-04T14:50:11-04:00
draft: false
weight: 4
---

This document aims to provide complete information needed for anyone who would like to contribute to the dlt-meta project.  Your contributions are vital to its success, whether you’re fixing bugs, improving documentation, or adding new features.

# Steps

## Step 0 \- Read the documentation

Refer documentation wiki page [here](https://databricks.atlassian.net/wiki/spaces/FE/pages/2985722046/DLT-META) that will guide you to access different DLT-META resources like documentation, github repo, presentation etc. Read the getting started link [here](https://databrickslabs.github.io/dlt-meta/getting_started/) to understand pre-requisite , setup steps and configuration details.

Prerequisite

* Install Databricks CLI to you local machine  
* Authenticate you current machine to a Databricks Workspace  
* Python 3.8.0+

## Step 1 \- Join the slack channel

\#dlt-meta

## Step 2 \- Fork the Repository

In case you may not be able to fork this repo because the repository is outside of your enterprise Databricks(EMU) , follow step3   or Fork using a personal github account.

## Step 3 \- Clone the Repository Locally

1. Run command “git clone [https://github.com/databrickslabs/dlt-meta.git](https://github.com/databrickslabs/dlt-meta.git)” it will create folder name “dlt-meta” 

## Step 4 \- Set Up the Development Environment

2. cd dlt-meta  
3. Create python virtual environment   
   * python \-m venv .venv or python3 \-m venv .venv  
4. Activate python virtual environment  
   * source .venv/bin/activate   
5. Install databricks sdk  
   * pip install databricks-sdk  
6. Install code editor like VS code or any other.  
7. Import project into VS code File \> Open folder \> select above dlt-meta folder from your system  
8. Install setuptools and wheel if not already installed  
   * pip install setuptools wheel  
9. Install the project dependencies specified in setup.py   
   * pip install \-e .  
10.  Build the project  
    * python setup.py sdist bdist\_wheel  
11. Install additional dependencies  
    * pip install pyspark  
    * pip install delta-spark  
    * Pip install pytest

## Step 5 \- Running Unit and Integration Tests

* Unit test are at tests folder  
  * To run the test cases, use the pytest command in the terminal  
  * To run all tests run \- pytest  
  * To run specific test- pytest \-k “test\_case\_name”

* Integration Tests are at integration\_tests folder  
  * To run integration test run file run\_integration\_tests.py with mandatory required argument as below  
  * e.g. run\_integration\_tests.py \--uc\_catalog\_name datta\_demo \--cloud\_provider\_name aws \--dbr\_version 14.3 \--source cloudfiles \--dbfs\_path "dbfs:/tmp/DLT-META/" \--profile DEFAULT

## Step 6 \- Find Beginner-Friendly Issues

## Step 7 \- Work on the Issue

## Step 8 \- Submit a PR

## Step 9 \- Celebrate your Contribution

