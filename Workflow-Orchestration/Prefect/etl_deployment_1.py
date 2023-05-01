from etl_1 import etl # Import Flow function
from prefect.deployments import Deployment

from datetime import timedelta

# Deployment configurations
deployment = Deployment.build_from_flow(
    flow=etl,
    name="etl-deployment-2",
)

if __name__ == "__main__":
    deployment.apply()
