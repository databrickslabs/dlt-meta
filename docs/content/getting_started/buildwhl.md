---
title: "Build Python Wheel"
date: 2021-08-04T14:25:26-04:00
weight: 16
draft: false
---

This step is optional if you use ```PyPI dlt-meta```
- Clone DLT-META repo locally
- Launch terminal
- Goto root dlt-meta folder
- use command: ```python setup.py bdist_wheel```
- Upload created python wheel file from dist folder to your blob storage like s3/adls/dbfs