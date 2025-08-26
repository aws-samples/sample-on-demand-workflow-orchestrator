from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_iam as iam,
    aws_lambda_event_sources as lambda_event_sources,
    aws_ssm as ssm,
    aws_ecr_assets as ecr_assets,
)
import json
from constructs import Construct
import os

class OnDemandOrchestratorStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create S3 bucket for payloads and outputs
        payload_bucket = s3.Bucket(
            self, "PayloadBucket",
            removal_policy=RemovalPolicy.DESTROY,  # For development only, use RETAIN for production
            auto_delete_objects=True,  # For development only, remove for production
            enforce_ssl=True,  # Enforce SSL/TLS for all requests
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="delete-after-10-days",
                    enabled=True,
                    expiration=Duration.days(10)
                )
            ]
        )

        # Create DynamoDB table for workflow tasks
        workflow_table = dynamodb.Table(
            self, "WorkflowTable",
            partition_key=dynamodb.Attribute(
                name="run_id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="task_id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            removal_policy=RemovalPolicy.DESTROY,  # For development only, use RETAIN for production
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(point_in_time_recovery_enabled=True),  # Enable point-in-time recovery
            time_to_live_attribute="ttl",  # Enable TTL with the ttl attribute
        )

        # Create SSM parameters for bucket and table names
        payload_bucket_param = ssm.StringParameter(
            self, "PayloadBucketParam",
            parameter_name="/on-demand-orchestrator/payload-bucket",
            description="S3 bucket name for on-demand orchestrator payloads",
            string_value=payload_bucket.bucket_name,
        )
        payload_bucket_param.apply_removal_policy(RemovalPolicy.DESTROY)
        
        workflow_table_param = ssm.StringParameter(
            self, "WorkflowTableParam",
            parameter_name="/on-demand-orchestrator/workflow-table",
            description="DynamoDB table name for on-demand orchestrator workflows",
            string_value=workflow_table.table_name,
        )
        workflow_table_param.apply_removal_policy(RemovalPolicy.DESTROY)

        # Create Lambda functions
        monitor_lambda_role = iam.Role(
            self, "MonitorLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        monitor_lambda_role.apply_removal_policy(RemovalPolicy.DESTROY)
        
        # Add CloudWatch Logs permissions (replacing AWSLambdaBasicExecutionRole)
        monitor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=["*"]
            )
        )

        # Add permissions to the Lambda execution role
        monitor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:Query",
                    "dynamodb:Scan"
                ],
                resources=[workflow_table.table_arn]
            )
        )
        
        # Add permissions to access SSM Parameter Store
        monitor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:GetParameter"
                ],
                resources=[
                    payload_bucket_param.parameter_arn,
                    workflow_table_param.parameter_arn
                ]
            )
        )

        monitor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                resources=[
                    payload_bucket.bucket_arn,
                    f"{payload_bucket.bucket_arn}/*"
                ]
            )
        )

        # Create Lambda functions
        execute_handler_lambda_role = iam.Role(
            self, "ExecuteHandlerLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        execute_handler_lambda_role.apply_removal_policy(RemovalPolicy.DESTROY)
        
        # Add CloudWatch Logs permissions (replacing AWSLambdaBasicExecutionRole)
        execute_handler_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=["*"]
            )
        )

        # Add permissions to the Lambda execution role
        execute_handler_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:Query",
                    "dynamodb:Scan"
                ],
                resources=[workflow_table.table_arn]
            )
        )
        
        # Add permissions to access SSM Parameter Store
        execute_handler_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:GetParameter"
                ],
                resources=[
                    payload_bucket_param.parameter_arn,
                    workflow_table_param.parameter_arn
                ]
            )
        )

        execute_handler_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                resources=[
                    payload_bucket.bucket_arn,
                    f"{payload_bucket.bucket_arn}/*"
                ]
            )
        )
        
        # Create a separate role for execute function with minimal permissions
        execute_lambda_role = iam.Role(
            self, "ExecuteLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        execute_lambda_role.apply_removal_policy(RemovalPolicy.DESTROY)
        
        # Add CloudWatch Logs permissions (replacing AWSLambdaBasicExecutionRole)
        execute_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                resources=["*"]
            )
        )
        execute_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "bedrock:InvokeModelWithResponseStream",
                ],
                resources=[
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/us.anthropic.claude-3-7-sonnet-20250219-v1:0",
                    f"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-7-sonnet-20250219-v1:0",
                    f"arn:aws:bedrock:us-east-2::foundation-model/anthropic.claude-3-7-sonnet-20250219-v1:0",
                    f"arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-3-7-sonnet-20250219-v1:0",
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/us.anthropic.claude-3-5-haiku-20241022-v1:0",
                    f"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0",
                    f"arn:aws:bedrock:us-east-2::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0",
                    f"arn:aws:bedrock:us-west-2::foundation-model/anthropic.claude-3-5-haiku-20241022-v1:0",
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/us.amazon.nova-pro-v1:0",
                    f"arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-pro-v1:0",
                    f"arn:aws:bedrock:us-east-2::foundation-model/amazon.nova-pro-v1:0",
                    f"arn:aws:bedrock:us-west-2::foundation-model/amazon.nova-pro-v1:0",
                    f"arn:aws:bedrock:{self.region}:{self.account}:inference-profile/us.amazon.nova-premier-v1:0",
                    f"arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-premier-v1:0",
                    f"arn:aws:bedrock:us-east-2::foundation-model/amazon.nova-premier-v1:0",
                    f"arn:aws:bedrock:us-west-2::foundation-model/amazon.nova-premier-v1:0",
                ]
            )
        )
        execute_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "ssm:GetParameter"
                ],
                resources=[
                    payload_bucket_param.parameter_arn,
                    workflow_table_param.parameter_arn
                ]
            )
        )
        execute_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:Query",
                    "dynamodb:Scan"
                ],
                resources=[workflow_table.table_arn]
            )
        )

        execute_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:ListBucket"
                ],
                resources=[
                    payload_bucket.bucket_arn,
                    f"{payload_bucket.bucket_arn}/*"
                ]
            )
        )

        

        # Create Lambda Layer from lib directory
        lambda_layer = lambda_.LayerVersion(
            self, "OrchestratorLayer",
            code=lambda_.Code.from_asset("lib"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            description="Common utilities for the on-demand orchestrator Lambda functions",
            layer_version_name="orchestrator-layer"
        )
        lambda_layer.apply_removal_policy(RemovalPolicy.DESTROY)

        # Create execute handler function 
        execute_handler_function = lambda_.Function(
            self, "ExecuteHandlerFunction",
            function_name="ExecuteHandlerFunction",
            runtime=lambda_.Runtime.PYTHON_3_13, 
            handler="index.handler",
            code=lambda_.Code.from_asset("src/functions/execute"),
            timeout=Duration.minutes(15),
            memory_size=512,
            role=execute_handler_lambda_role,
            environment={},
            layers=[lambda_layer],  # Add the Lambda layer
        )
        execute_handler_function.apply_removal_policy(RemovalPolicy.DESTROY)

        # Create execute agent function 
        execute_agent_function = lambda_.DockerImageFunction(
            self, "ExecuteAgentFunction",
            function_name="ExecuteAgentFunction",
            code=lambda_.DockerImageCode.from_image_asset(
                directory="src/functions/execute_agent",
                platform=ecr_assets.Platform.LINUX_AMD64
            ),
            timeout=Duration.minutes(15),
            role=execute_lambda_role,
            environment={},
            memory_size=512
        )
        execute_agent_function.apply_removal_policy(RemovalPolicy.DESTROY)

        # Create execute math function 
        execute_math_function = lambda_.Function(
            self, "ExecuteMathFunction",
            function_name="ExecuteMathFunction",
            runtime=lambda_.Runtime.PYTHON_3_13, 
            handler="index.handler",
            code=lambda_.Code.from_asset("src/functions/execute_math"),
            timeout=Duration.minutes(15),
            memory_size=512,
            role=execute_lambda_role,
            environment={},
            layers=[lambda_layer],  # Add the Lambda layer
        )
        execute_math_function.apply_removal_policy(RemovalPolicy.DESTROY)

        # Add permissions after all functions are created
        execute_handler_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[
                    f"{execute_math_function.function_arn}", 
                    f"{execute_agent_function.function_arn}",
                ]
            )
        )

        # Create monitor function
        monitor_function = lambda_.Function(
            self, "MonitorFunction",
            runtime=lambda_.Runtime.PYTHON_3_13, 
            handler="index.handler",
            code=lambda_.Code.from_asset("src/functions/monitor"),
            timeout=Duration.minutes(15),
            memory_size=512,
            role=monitor_lambda_role,
            environment={
                "TASK_FUNCTION_NAME": execute_math_function.function_name  # default function to execute
            },
            layers=[lambda_layer],  # Add the Lambda layer
        )
        monitor_function.apply_removal_policy(RemovalPolicy.DESTROY)
        
        # Add permissions after all functions are created
        monitor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[
                    f"{execute_handler_function.function_arn}"
                ]
            )
        )

        # Add DynamoDB stream as event source for monitor function after all resources are defined
        event_source = lambda_event_sources.DynamoEventSource(
            table=workflow_table,
            starting_position=lambda_.StartingPosition.LATEST,
            batch_size=1,
            retry_attempts=3,
            parallelization_factor=10
        )
        
        monitor_function.add_event_source(event_source)
