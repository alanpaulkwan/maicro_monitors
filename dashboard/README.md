# Dashboard (Streamlit)

How to run the dashboard locally and secure it with a password.

## Requirements
- Python (3.10+ recommended)
- Streamlit, pandas, numpy, clickhouse-driver, plotly

Install packages (if not already installed):

```bash
pip install streamlit clickhouse-driver pandas numpy plotly
```

## Run the dashboard

Run the Streamlit app:

```bash
streamlit run 06_dashboards/streamlit_main.py --server.port 8501 --server.headless true
```

## Securing with a password

The app supports a simple optional password gate. Set the `MAICRO_DASH_PASSWORD` environment variable (or `DASHBOARD_PASSWORD`) to require a password before the app renders.

Example:

```bash
export MAICRO_DASH_PASSWORD="MyStrongPassword"
streamlit run 06_dashboards/streamlit_main.py --server.port 8501 --server.headless true
```

When configured, the dashboard shows a password input in the sidebar. The application does not reveal sensitive information until authentication succeeds.

If `MAICRO_DASH_PASSWORD` is not set, the dashboard will run without authentication and will display a warning in the sidebar.

---
Notes:
- This is a simple convenience gate and not a full auth system; for production deployments, put the dashboard behind a proper authentication proxy (nginx/basic auth, OAuth, or a Cloud Load Balancer with auth).
