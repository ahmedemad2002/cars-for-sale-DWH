# dbt runner Lambda — container image
# Build from the REPO ROOT so the dbt project is in the build context:
#   docker build -f "Lambda Scripts/dbt-runner/Dockerfile" -t dubizzle-dbt-runner .
FROM public.ecr.aws/lambda/python:3.12

# The tracking cookie would be written to the read-only home dir in Lambda
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false

RUN pip install --no-cache-dir "dbt-core~=1.11.11" "dbt-athena-community~=1.10.1"

# LAMBDA_TASK_ROOT is /var/task in this base image; JSON-form COPY (needed for
# the space in "Lambda Scripts") cannot expand variables, so it's used literally.
COPY dubizzle_dbt/ /var/task/dubizzle_dbt/
COPY ["Lambda Scripts/dbt-runner/profiles.yml", "/var/task/profiles/profiles.yml"]

# Pre-install dbt packages at build time — the Lambda filesystem is read-only.
RUN cd /var/task/dubizzle_dbt && dbt deps --profiles-dir /var/task/profiles

COPY ["Lambda Scripts/dbt-runner/handler.py", "/var/task/handler.py"]

CMD ["handler.lambda_handler"]
