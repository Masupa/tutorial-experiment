from etl_gcs_to_big_query_3 import etl
from prefect.deployments import Deployment


etl_deployment = Deployment.build_from_flow(
    flow=etl,
    name="ETL Deployment - GCS to BigQuery",
)

if __name__ == "__main__":
    etl_deployment.apply()
