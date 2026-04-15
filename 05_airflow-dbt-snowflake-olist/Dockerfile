FROM apache/airflow:3.0.4

# 1. Switch to root to perform system-level installations
USER root

# 2. Copy your dependency files
COPY ./airflow/pyproject.toml ./airflow/uv.lock ./

# 3. Install packages using uv into the system Python
RUN uv pip install -r ./pyproject.toml --group dbt --refresh

# 4. Install dbt dependencies from the sibling folder
# We point to the dbt folder relative to the build context
# COPY ./dbt/requirements.txt ./dbt-requirements.txt
# RUN uv pip install --requirement ./dbt-requirements.txt 

# 5. IMPORTANT: Switch back to the airflow user
# If you don't do this, the Airflow services will run as root, 
# which causes permission issues with logs and volumes later.
USER airflow
