#!/bin/bash
# shellcheck disable=SC2006

# Exit immediately if a command exits with a non-zero status
set -e


# Load environment variables from .env file
if [ -f .env ]; then
  # Using source
  source .env

  # Alternatively, you can use the dot command
  # . .env
fi

# Now you can use the environment variables
echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"
echo "AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION"
echo "STACK_NAME: $STACK_NAME"

# Set AWS credentials as environment variables
export AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
export AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION


# Notify the user that the stack update process is starting
printf '\nUpdating stack...\n\n'

# Define the CloudFormation template file name and file path
stack_yml="stack.yml"
stack=$STACK_NAME
# Display the stack name being processed
echo "Stack: $stack"

# Check if the stack exists by attempting to describe it
stack_status=$(aws cloudformation describe-stacks --stack-name "$stack" --query "Stacks[0].StackStatus" --output text 2>/dev/null || echo -1)

if [ "$stack_status" = "-1" ]; then
    # Stack does not exist, create it
    echo "Creating a new stack: $stack"
    aws cloudformation create-stack --stack-name "$stack" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml"
    echo "Waiting for stack creation to complete: $stack"
    aws cloudformation wait stack-create-complete --stack-name "$stack"
    status=$?
elif [ "$stack_status" = "ROLLBACK_COMPLETE" ]; then
    # Stack is in ROLLBACK_COMPLETE, must delete and recreate
    echo "Stack $stack is in ROLLBACK_COMPLETE. Deleting it before recreating."
    aws cloudformation delete-stack --stack-name "$stack"
    echo "Waiting for stack deletion to complete: $stack"
    aws cloudformation wait stack-delete-complete --stack-name "$stack"
    echo "Recreating stack: $stack"
    aws cloudformation create-stack --stack-name "$stack" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml"
    echo "Waiting for stack creation to complete: $stack"
    aws cloudformation wait stack-create-complete --stack-name "$stack"
    status=$?
else
    # Stack exists and is not in ROLLBACK_COMPLETE, update it
    echo "Updating the stack: $stack"
    aws cloudformation update-stack --stack-name "$stack" \
        --capabilities CAPABILITY_NAMED_IAM CAPABILITY_AUTO_EXPAND \
        --template-body file://"$stack_yml"
    echo "Waiting for stack update to complete: $stack"
    aws cloudformation wait stack-update-complete --stack-name "$stack"
    status=$?
fi

# Check the status of the operation (creation or update)
if [[ $status -ne 0 ]]; then
    # If the command failed, output an error message and exit with the same status code
    echo "$stack operation failed with AWS error code: $status."
    exit $status
else
    # If the command succeeded, notify the user
    echo "$stack operation completed successfully."
fi