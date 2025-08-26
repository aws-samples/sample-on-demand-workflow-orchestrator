#!/usr/bin/env python3
import os
import aws_cdk as cdk
from cdk.on_demand_orchestrator_stack import OnDemandOrchestratorStack
from aws_cdk import Aspects # for CDK NAG
import cdk_nag # for CDK NAG
from cdk_nag import NagSuppressions # for CDK NAG

app = cdk.App()
stack = OnDemandOrchestratorStack(app, "OnDemandOrchestratorStack",env=cdk.Environment(region="us-east-1"))

Aspects.of(app).add(cdk_nag.AwsSolutionsChecks(verbose=True)) # for CDK NAG

NagSuppressions.add_stack_suppressions(stack, [ # for CDK NAG
    { "id": 'AwsSolutions-S1', "reason": 'The S3 bucket is used to store payload temporarily and only accessed by the library SDK.'},
    { "id": 'AwsSolutions-IAM5', "reason": '* required as part of granting access to all S3 objects within the payload bucket.'}
])

app.synth()
