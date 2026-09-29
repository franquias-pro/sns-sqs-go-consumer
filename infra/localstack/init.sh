#!/bin/sh
set -eu

topic_arn=$(awslocal sns create-topic --name example-events --query TopicArn --output text)
dlq_url=$(awslocal sqs create-queue --queue-name example-events-dlq --query QueueUrl --output text)
dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)
queue_url=$(awslocal sqs create-queue --queue-name example-events --attributes VisibilityTimeout=120,ReceiveMessageWaitTimeSeconds=20 --query QueueUrl --output text)
queue_arn=$(awslocal sqs get-queue-attributes --queue-url "$queue_url" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

export TOPIC_ARN="$topic_arn" QUEUE_ARN="$queue_arn" DLQ_ARN="$dlq_arn"
python3 - <<'PY' > /tmp/queue-attributes.json
import json, os
policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow", "Principal": {"Service": "sns.amazonaws.com"},
        "Action": "sqs:SendMessage", "Resource": os.environ["QUEUE_ARN"],
        "Condition": {"ArnEquals": {"aws:SourceArn": os.environ["TOPIC_ARN"]}},
    }],
}
print(json.dumps({
    "Policy": json.dumps(policy),
    "RedrivePolicy": json.dumps({"deadLetterTargetArn": os.environ["DLQ_ARN"], "maxReceiveCount": "5"}),
}))
PY

awslocal sqs set-queue-attributes --queue-url "$queue_url" --attributes file:///tmp/queue-attributes.json
awslocal sns subscribe --topic-arn "$topic_arn" --protocol sqs --notification-endpoint "$queue_arn" --attributes RawMessageDelivery=true
