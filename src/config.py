import os


class Config:
    CloudWatchPeriod = 60
    AWSRegion = "eu-west-1"
    UtilizationLevel = 0.70

    def __init__(self):
        if "Cloudwatch_Period" in os.environ and os.environ["Cloudwatch_Period"] is not None and os.environ["Cloudwatch_Period"] is not "":
            self.CloudWatchPeriod = int(os.environ["Cloudwatch_Period"])

        if "Aws_Region" in os.environ and os.environ["Aws_Region"] is not None and os.environ["Aws_Region"] is not "":
            self.AWSRegion = os.environ["Aws_Region"]

        if "Utilization_Level" in os.environ and os.environ["Utilization_Level"] is not None and os.environ["Utilization_Level"] is not "":
            self.UtilizationLevel = float(os.environ["Utilization_Level"])
