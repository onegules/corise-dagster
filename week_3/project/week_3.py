from typing import Dict, List

from dagster import (
    In,
    Nothing,
    OpExecutionContext,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    SensorEvaluationContext,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock

Stocks = List[Stock]

@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=Stocks)},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context: OpExecutionContext) -> Stocks:
    stocks = []
    for row in context.resources.s3.get_data(context.op_config["s3_key"]):
        stock = Stock.from_list(row)
        stocks.append(stock)

    return stocks

@op(
    ins={"stocks": In(dagster_type=Stocks)},
    out={"Aggregation": Out(dagster_type=Aggregation)},
    description="Find stock with highest value according to the 'high' field",
)
def process_data(stocks: Stocks) -> Aggregation:
    highest = sorted(stocks, key=lambda stock: stock.high, reverse=True)[0]
    aggregation = Aggregation(date=highest.date, high=highest.high)
    return aggregation


@op(tags={"kind": "redis"},
        ins={"aggregation": In(dagster_type=Aggregation)},
        required_resource_keys={"redis"},
        description="Upload aggregation to Redis.",
)
def put_redis_data(context, aggregation: Aggregation) -> Nothing:
    context.resources.redis.put_date(str(aggregation.date), str(aggregation.high))

@graph
def week_3_pipeline():
    stocks = get_s3_data()
    aggregation = process_data(stocks)
    put_redis_data(aggregation)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=[str(num) for num in range(1,11)])
def docker_config(partition_key: str) -> Dict:
    config = docker.copy()
    config["ops"]["get_s3_data"]["config"]["s3_key"] = f"prefix/stock_{partition_key}.csv"
    return config


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *") 

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *") 


@sensor(job=docker_week_3_pipeline)
def docker_week_3_sensor(context: SensorEvaluationContext):
    endpoint_url = docker["resources"]["s3"]["config"]["endpoint_url"]
    new_files = get_s3_keys(bucket="dagster", prefix="prefix", endpoint_url=endpoint_url)
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    
    for new_file in new_files:
        config = docker.copy()
        config["ops"]["get_s3_data"]["config"]["s3_key"] = new_file
        yield RunRequest(run_key=new_file, run_config=config)
