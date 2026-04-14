FROM apache/airflow:3.0.4

# 1. Switch to root to perform system-level installations
USER root

# 2. Copy your dependency files
COPY pyproject.toml uv.lock ./

# 3. Install packages using uv into the system Python
RUN uv pip install --requirement pyproject.toml --refresh

# 4. IMPORTANT: Switch back to the airflow user
# If you don't do this, the Airflow services will run as root, 
# which causes permission issues with logs and volumes later.
USER airflow