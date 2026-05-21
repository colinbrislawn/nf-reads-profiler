---
name: deploy-stack
description: Deploy the CloudFormation stack and re-validate Batch compute environments
---

# Deploy the nf-reads-profiler Batch stack

This skill deploys the CloudFormation stack and re-validates compute environments.

**Before running**: confirm with the user that they want to deploy. Show them the
`git diff infra/batch-stack.yaml` so they can review changes.

## Steps

### 1. Look up the current custom AMI ID

The custom AMI is built by `infra/packer/build-ami.sh` and its ID is stored
in SSM. If SSM is not accessible from the head node, fall back to querying EC2:

```bash
# Try SSM first
AMI_ID=$(aws ssm get-parameter --name /nf-reads-profiler/ami-id --region us-east-2 \
  --query 'Parameter.Value' --output text 2>/dev/null)

# Fallback: latest self-owned AMI
if [ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ]; then
  AMI_ID=$(aws ec2 describe-images --region us-east-2 --owners self \
    --filters "Name=name,Values=nf-reads-profiler-worker-*" "Name=state,Values=available" \
    --query 'Images | sort_by(@,&CreationDate) | [-1].ImageId' --output text)
fi
echo "AMI: $AMI_ID"
```

Show the AMI ID to the user for confirmation before proceeding.

### 2. Validate the template

```bash
aws cloudformation validate-template \
  --template-body file://infra/batch-stack.yaml \
  --region us-east-2
```

If validation fails, stop and report the error.

### 3. Deploy the stack

```bash
aws cloudformation deploy \
  --stack-name nf-reads-profiler-batch \
  --template-file infra/batch-stack.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-2 \
  --parameter-overrides \
    VpcId=vpc-06ad1e39bb8cd26df \
    SubnetIds="subnet-09159c654acc505a3,subnet-03afe111356916511,subnet-0d0f1d152c1656677" \
    WorkDirBucketName=gutz-nf-reads-profilers-workdir \
    RunsBucketName=gutz-nf-reads-profilers-runs \
    BudgetAlertEmail=colin@vasogo.com \
    MonthlyBudgetThreshold=200 \
    SpotBidPercentage=50 \
    MaxvCPUsSpot=256 \
    MaxvCPUsOnDemand=0 \
    ProjectTag=nf-reads-profiler \
    EnvironmentTag=development \
    DbSourceBucket=cjb-gutz-s3-demo \
    EcsAmiId=$AMI_ID
```

### 4. Wait for completion

```bash
aws cloudformation wait stack-update-complete \
  --stack-name nf-reads-profiler-batch \
  --region us-east-2
```

### 5. Force compute environments to pick up the new launch template

**Important:** A simple disable/re-enable does NOT force Batch to re-snapshot
the launch template UserData. You must explicitly update the CEs with the
launch template reference and `updateToLatestImageVersion`.

```bash
LT_ID=$(aws ec2 describe-launch-templates --region us-east-2 \
  --filters "Name=launch-template-name,Values=nf-reads-profiler-worker" \
  --query 'LaunchTemplates[0].LaunchTemplateId' --output text)

CE_SPOT=$(aws batch describe-job-queues --job-queues spot-queue --region us-east-2 \
  --query "jobQueues[0].computeEnvironmentOrder[0].computeEnvironment" --output text)
CE_ONDEMAND=$(aws batch describe-job-queues --job-queues spot-queue --region us-east-2 \
  --query "jobQueues[0].computeEnvironmentOrder[1].computeEnvironment" --output text)

aws batch update-compute-environment --compute-environment $CE_SPOT --region us-east-2 \
  --compute-resources "{\"launchTemplate\":{\"launchTemplateId\":\"$LT_ID\",\"version\":\"\$Latest\"},\"updateToLatestImageVersion\":true}"
aws batch update-compute-environment --compute-environment $CE_ONDEMAND --region us-east-2 \
  --compute-resources "{\"launchTemplate\":{\"launchTemplateId\":\"$LT_ID\",\"version\":\"\$Latest\"},\"updateToLatestImageVersion\":true}"
```

Wait for both CEs to become VALID before proceeding.

### 6. Confirm both are VALID

```bash
aws batch describe-compute-environments \
  --region us-east-2 \
  --query "computeEnvironments[].{Name:computeEnvironmentName,State:state,Status:status}"
```

Report the result to the user.
