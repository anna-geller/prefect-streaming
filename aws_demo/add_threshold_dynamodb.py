import boto3

dynamodb = boto3.client("dynamodb")
dynamodb.put_item(
    TableName="crypto", Item={"coin": {"S": "btc"}, "threshold": {"S": "18000"}}
)
