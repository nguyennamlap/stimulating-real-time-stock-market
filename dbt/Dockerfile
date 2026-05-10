FROM ghcr.io/dbt-labs/dbt-postgres:1.7.0

WORKDIR /app

COPY stock_dbt_project /app
COPY profiles /root/.dbt

# Install dbt packages
RUN dbt deps --profiles-dir /root/.dbt

# Default command (có thể override)
CMD ["run", "--profiles-dir", "/root/.dbt"]