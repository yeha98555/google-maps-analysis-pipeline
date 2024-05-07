### Execution
```shell
# Initialize state file (.tfstate)
terraform init

# List all workspaces
terraform workspace list

# Create a new workspace
terraform workspace new dev

# Select a workspace
terraform workspace select dev

# Need to Enable IAM.API and Cloud Resource Manager.API first
# Check changes to new infra plan
terraform plan

# Create new infra
terraform apply

# Verify the workspace
terraform workspace show

# Delete infra after your work, to avoid costs on any running services
terraform destroy
```
