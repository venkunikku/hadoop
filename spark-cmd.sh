#!/usr/bin/env bash

export AWS_ACCESS_KEY_ID=''
export AWS_SECRET_ACCESS_KEY=''
spark-ec2/spark-ec2 -k NAME_OF_KEY_PAIR --identity-file=PATH_TO_PEM_FILE --region=us-west-2 \
    --zone=us-west-2a --copy-aws-credentials --instance-type t2.micro --worker-instances 1 launch NAME_OF_YOUR_CLUSTER
