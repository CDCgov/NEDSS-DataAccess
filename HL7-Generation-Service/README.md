# HL7v2.5.1 ORU RO1 Pandemic Simulation Message Generator

This Python script is designed to generate HL7 version 2.5.1 Observation Result (ORU) messages for testing the readiness of a healthcare system during a pandemic. The script is intended to simulate the generation of messages by disease types, allowing for configurable parameters such as jurisdiction, disease code, and the number of Electronic Laboratory Reports (ELRs).

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Contributing](#contributing)

## Introduction

The HL7v2.5.1 Pandemic Simulation Message Generator is a Python script tailored for testing the pandemic readiness of a healthcare system. It focuses on generating HL7 messages with specific disease types, providing configurable parameters(for jurisdiction, disease type, number of ELRS), and simulating pandemic scenarios.

## Features

These are the features for Version 1
- Generation of HL7v2.5.1 messages by disease types.
- Configurable parameters for jurisdiction, disease code, and the number of ELRs.
- Simulates pandemic scenarios with 1) batch processing 2) send 10k messages per second.
- Updates existing messages instead of always creating new ones.
- Integration with a DI service for configurability.
- UI for monitoring submitted messages and status updates for the National Bio-surveillance (NBS) system.
- Database configurability to avoid direct insertion.

## Installation

Ensure you have Python installed. Clone the repository and install dependencies.
- https://www.python.org/downloads/
- https://pypi.org/project/Faker/
- https://pypi.org/project/boto3/
- https://pypi.org/project/requests/
- https://pypi.org/project/aiohttp/

## Required libraries for the deployment in AWS Lambda

The following libraries should be included in the source and zipped for deployment.
- Faker
- boto3
- requests
- aiohttp