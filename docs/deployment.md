```markdown
# Deployment

ShopSphere Databricks resources are managed using Declarative Automation Bundles.

## Validate

```bash
cd databricks_bundle

databricks bundle validate \
  -t dev \
  --profile shopsphere-dev