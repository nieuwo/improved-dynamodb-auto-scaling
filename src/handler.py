import boto3
from datetime import datetime, timedelta, timezone
from src.config import Config
import json


def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            message = json.loads(record["Sns"]["Message"])
            alarmName = message["AlarmName"]
            namespace = message["Trigger"]["Namespace"]
            metricName = message["Trigger"]["MetricName"]
            tableName = message["Trigger"]["Dimensions"][0]["value"]
            snsTopic = record["EventSubscriptionArn"]
            cfg = Config()
            cloudwatch = boto3.client("cloudwatch", region_name=cfg.AWSRegion)

            response = cloudwatch.get_metric_statistics(
                Namespace=namespace,
                MetricName=metricName,
                Dimensions=[{"Name": "TableName", "Value": tableName}],
                StartTime=datetime.utcnow() - timedelta(minutes=5),
                EndTime=datetime.utcnow(),
                Period=cfg.CloudWatchPeriod,
                Statistics=["Sum"])

            maxReqsCount = max([dp["Sum"]
                                for dp in response["Datapoints"]])
            newThroughput = round(
                min(1000, maxReqsCount / cfg.UtilizationLevel / cfg.CloudWatchPeriod))

            newThreshold = round(
                (newThroughput * cfg.CloudWatchPeriod * cfg.UtilizationLevel))

            dynamodb = boto3.client("dynamodb", region_name=cfg.AWSRegion)

            table = dynamodb.describe_table(TableName=tableName)
            lastDecreaseDateTime = table["Table"]["ProvisionedThroughput"]["LastDecreaseDateTime"]
            lastDecreaseTime = (datetime.now(tz=timezone.utc) - lastDecreaseDateTime) / timedelta(minutes=1)
            oldWriteCapacityUnits = table["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"]
            oldReadCapacityUnits = table["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"]

            if (lastDecreaseTime > 60 and newThroughput < oldWriteCapacityUnits) or newThroughput > oldWriteCapacityUnits:
                print("%s newThroughput [%s] vs old [%s]" %
                      (tableName, newThroughput, oldWriteCapacityUnits))

                dynamodb.update_table(TableName=tableName, ProvisionedThroughput={
                                      "WriteCapacityUnits": newThroughput, "ReadCapacityUnits": oldReadCapacityUnits})

                cloudwatch.put_metric_alarm(AlarmName=alarmName, Namespace=namespace, MetricName=metricName, EvaluationPeriods=1, AlarmActions=[
                                            snsTopic], Period=int(message["Trigger"]["Period"]), Threshold=newThroughput, ComparisonOperator=message["Trigger"]["ComparisonOperator"], Statistic="Sum")

                cloudwatch.set_alarm_state(
                    AlarmName=alarmName, StateValue="OK", StateReason="Updated by dynamodb-autoscale")

    except Exception as e:
        print(e)
