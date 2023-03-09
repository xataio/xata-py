# Reading and Storing Stock Prices from RabbitMQ in Xata

This example is intended to show you how to read events from a [RabbitMQ](https://rabbitmq.com) queue and store them in [Xata](https://xata.io). 
We are generating fictive companies and simulate stock prices for each company, whom move up and down with each tick.

A **producer** creates the companies and pushes every `env.N_TICK_INTERVAL` seconds stock price changes to a queue.

Multiple **consumers** read from the queue and ingest the data in Xata with the [`BulkProcessor`](https://xata.io/docs/python-sdk/bulk-processor).

## Prerequisites

- You need to have a Xata account, please use this link [here](https://app.xata.io/) to create one. You can use your Gmail or GitHub accounts to sign in.
- An API Key to access Xata, please follow the instructions [how to generate an API key](https://xata.io/docs/getting-started/api-keys).

## Getting Started

Three steps are required in order to run this example:

1. Rename `.env.sample` to `.env`, or make a copy.
2. Personalize the values for `XATA_API_KEY` and `XATA_WORKSPACE_ID` in the `.env`.
3. Run `make setup`. This will create the necessary tables.

You're all set to run the example!

## Run the Example

Open the Xata UI to see data trickle in.

In a terminal window, enter: `make run` and the 

 