# de_project
A data engineering project using Azure Databricks, Delta Lake, and ADLS Gen2  implementing medallion architecture. Includes an AI agent using MCP protocol  for natural language to SQL querying of live flight data from OpenSky Network API.

## Features

- **Medallion Architecture**: Implements bronze, silver, and gold layers for scalable and reliable data pipelines.
- **Azure Databricks**: Utilizes Databricks notebooks and jobs for ETL workflows.
- **Delta Lake**: Ensures ACID transactions and scalable metadata handling.
- **ADLS Gen2 Integration**: Stores raw and processed data securely and efficiently.
- **AI Agent (MCP Protocol)**: Converts natural language queries to SQL for live analytics.
- **OpenSky Network API**: Ingests real-time flight data for analysis.

## Project Structure

```
/de_project
│
├── notebooks/           # Databricks notebooks for ETL and analytics
├── src/                 # Source code for data ingestion and transformation
├── configs/             # Configuration files
├── ai_agent/            # MCP protocol agent for NL-to-SQL
├── data/                # Sample datasets and schema definitions
└── README.md
```

## Getting Started

1. **Clone the repository**
    ```bash
    git clone https://github.com/yourusername/de_project.git
    ```

2. **Set up Azure Databricks and ADLS Gen2**
    - Provision Databricks workspace and ADLS Gen2 storage.
    - Configure secrets and mount points as needed.

3. **Configure the OpenSky Network API**
    - Obtain API credentials from [OpenSky Network](https://opensky-network.org/).
    - Update configuration files in `configs/`.

4. **Run ETL Pipelines**
    - Import notebooks into Databricks and execute in order: Bronze → Silver → Gold.

5. **Use the AI Agent**
    - Start the MCP protocol agent for natural language querying.

## Example Query

> "Show me all flights flying from India"

The AI agent translates this to SQL and queries the gold layer for results.

## License

This project is licensed under the MIT License.

## Acknowledgments

- [Azure Databricks](https://azure.microsoft.com/en-us/products/databricks/)
- [Delta Lake](https://delta.io/)
- [OpenSky Network](https://opensky-network.org/)