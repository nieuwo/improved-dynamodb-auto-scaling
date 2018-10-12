import boto3
from datetime import datetime, timedelta, timezone
from src.config import Config
import json


def lambda_handler(event, context):
    try:
        for record in event["Records"]:
            minIncrementor = 5
            message = json.loads(record["Sns"]["Message"])
            alarmName = message["AlarmName"]
            namespace = message["Trigger"]["Namespace"]
            metricName = message["Trigger"]["MetricName"]
            tableName = message["Trigger"]["Dimensions"][0]["value"]
            snsTopic = record["Sns"]["TopicArn"]
            cfg = Config()
            cloudwatch = boto3.client("cloudwatch",
                                      region_name=cfg.AWSRegion)
            print(metricName)
            response = cloudwatch.get_metric_statistics(Namespace=namespace,
                                                        MetricName=metricName,
                                                        Dimensions=[{"Name": "TableName", "Value": tableName}],
                                                        StartTime=datetime.now() - timedelta(minutes=minIncrementor),
                                                        EndTime=datetime.now(),
                                                        Period=cfg.CloudWatchPeriod,
                                                        Statistics=["Sum"])

            while len(response["Datapoints"]) == 0:
                minIncrementor = minIncrementor + 15
                response = cloudwatch.get_metric_statistics(Namespace=namespace,
                                                            MetricName=metricName,
                                                            Dimensions=[{"Name": "TableName", "Value": tableName}],
                                                            StartTime=datetime.now() - timedelta(minutes=minIncrementor),
                                                            EndTime=datetime.now(),
                                                            Period=cfg.CloudWatchPeriod,
                                                            Statistics=["Sum"])

            maxReqsCount = max([dp["Sum"] for dp in response["Datapoints"]])
            newThroughput = round(
                min(1000, maxReqsCount / cfg.UtilizationLevel / cfg.CloudWatchPeriod))
            if newThroughput == 0:
                newThroughput = 1

            newThreshold = round(
                (newThroughput * cfg.CloudWatchPeriod * cfg.UtilizationLevel))

            provisionedWriteThroughput = newThroughput
            provisionedReadThroughput = newThroughput

            dynamodb = boto3.client("dynamodb",
                                    region_name=cfg.AWSRegion)

            table = dynamodb.describe_table(TableName=tableName)
            lastDecreaseDateTime = table["Table"]["ProvisionedThroughput"]["LastDecreaseDateTime"]
            lastDecreaseTime = (datetime.now(tz=timezone.utc) - lastDecreaseDateTime) / timedelta(minutes=1)
            updateAlarms = False
            if "WriteCapacityUnits" in metricName:
                oldThroughput = table["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"]
                provisionedReadThroughput = table["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"]
                updateAlarms = True
            elif "ReadCapacityUnits" in metricName:
                oldThroughput = table["Table"]["ProvisionedThroughput"]["ReadCapacityUnits"]
                provisionedWriteThroughput = table["Table"]["ProvisionedThroughput"]["WriteCapacityUnits"]
                updateAlarms = True

            if updateAlarms:
                updateDecreasedThroughPut = newThroughput < oldThroughput
                updateIncreaseThroughPut = newThroughput > oldThroughput

                if (lastDecreaseTime > cfg.CloudWatchPeriod and updateDecreasedThroughPut) or updateIncreaseThroughPut:
                    print("%s newThroughput [%s] vs old [%s]" % (tableName, newThroughput, oldThroughput))

                    dynamodb.update_table(TableName=tableName,
                                          ProvisionedThroughput={"WriteCapacityUnits": provisionedWriteThroughput, "ReadCapacityUnits": provisionedReadThroughput})

                    cloudwatch.put_metric_alarm(AlarmName=alarmName,
                                                ActionsEnabled=True,
                                                AlarmActions=[snsTopic],
                                                MetricName=metricName,
                                                Namespace=namespace,
                                                Statistic="Sum",
                                                Dimensions=[{"Name": "TableName", "Value": tableName}],
                                                Period=int(message["Trigger"]["Period"]),
                                                EvaluationPeriods=1,
                                                Threshold=newThreshold,
                                                ComparisonOperator=message["Trigger"]["ComparisonOperator"])

                    cloudwatch.set_alarm_state(AlarmName=alarmName,
                                               StateValue="OK",
                                               StateReason="Updated by dynamodb-autoscale")

    except Exception as e:
        print(e)
