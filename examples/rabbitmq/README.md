# Reading and Storing Stock Prices from RabbitMQ

what it does start a rabbitmq instance, producer for company names and symbols, e.g.
```
'id': symbol,
                'name': name,
                'address': fake.address(),
                'catch_phrase': fake.catch_phrase(),
                'ceo': fake.name(),
                'phone': fake.phone_number(),
                'email': fake.company_email(),
                'exchange': random.choice(EXCHANGES),
```

`created_at` has default value `NOW()` view: https://xata.io/docs/concepts/data-model#datetime

and simulate stock price movements.
```
'@timestamp': datetime.now(timezone.utc).astimezone().isoformat(),
'symbol': string,
            'price': round(randrange(100, 9999) / randrange(9, 99), 3),
            'delta': 0.0,
            'percentage': 0.0,
```

Symbol is a link to `companies.id` view https://xata.io/docs/concepts/data-model#link


tweak the generated data in docker-compose.yml
- N_COMPANIES: 25 # how many companies to generate
- N_TICK_INTERVAL: 15 # how many seconds to wait between the new prices

some calculations on consumers with threads necessary
- add env var for threads
- scaling of workers --> docker-compose.yml

consumers read from queue and expect the format:
```
{
    "table": "<name of the table to ingest in>",
    "record": "<record payload>"
}
```



set the environment variables in docker-compose.yml

run the app `make run`

## Config

N_companies * N_ticks = data produced
`N_COMPANIES`: 100
`N_TICK_INTERVAL`: 15

Every minute: 100 * (60 / 15) = 400 records are created.

## Install

run `make setup` to create all tables in Xata

## Delete Everything from Xata

TODO


## Dead Letter Queue

failed documents should be pushed from the `failed_batches` to a DLQ in rabbitmq
