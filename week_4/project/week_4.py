from typing import List

from dagster import Nothing, asset, with_resources, OpExecutionContext
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock

Stocks = List[Stock]

@asset(
    group_name="corise",
    required_resource_keys={"s3"},
    op_tags={"kind":"s3"},
    config_schema={"s3_key": str},
    dagster_type=Stocks,
)
def get_s3_data(context: OpExecutionContext) -> Stocks:
    stocks = []
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        stocks.append(stock)

    return stocks

@asset(
    group_name="corise",
    description="Find stock with highest value according to the 'high' field",
        )
def process_data(get_s3_data) -> Aggregation:
    highest = sorted(get_s3_data, key=lambda stock: stock.high, reverse=True)[0]
    aggregation = Aggregation(date=highest.date, high=highest.high)
    return aggregation

@asset(
    group_name="corise",
    required_resource_keys={"redis"},
    op_tags={"kind":"redis"},
    description="Upload aggregation to Redis.",
        )
def put_redis_data(context, process_data) -> Nothing:
    context.resources.redis.put_date(str(process_data.date), str(process_data.high))

get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
        definitions=[get_s3_data, process_data, put_redis_data],
        resource_defs={"redis": redis_resource, "s3": s3_resource},
        resource_config_by_key={
            "s3": {
                "config":
                    {
                    "bucket": "dagster",
                    "access_key": "test",
                    "secret_key": "test",
                    "endpoint_url": "http://localstack:4566",
                        }
                },
            "redis": {
                "config":
                {
                    "host": "redis",
                    "port": 6379,
                }

                }
            }
        )
