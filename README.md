# Formula1 project with Azure Databricks

## This is a project that uses azure databricks to do analysis on formula1 drivers results and teams.

![photo1](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/13d15643-eb31-4bcd-81ee-de60ccc6be63)


1. In this project we ingest data from ergast-api (https://ergast.com/mrd/) that documents all races from formula1 since 1950
2. Save the data to azure datalake storage 
3. Transform the data using databricks notebooks to make the data ready for presentation 
4. Present datasets with race results , driver standings , team standings , etc ...


Architecture Used is the common medallion architecture.
A medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables). Medallion architectures are sometimes also referred to as "multi-hop" architectures.

![photo2](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/feda489a-6a01-4fb2-b701-2a0c976f7a81)

5. Later on the project the datalake is transformed into the new delta lake architecture for even better data management.

![photo3](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/3b55ccb8-90a3-4d42-8329-f24fe97cc0eb)


6. A couple of visualizations are made in the databricks notebooks of the dominant drivers and teams for a better understanding of the subjects that we explore.
7. 
![photo4](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/2c445fa0-9890-40c6-9c60-41c8eedde89e)

![photo5](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/98958f36-6e6c-4f3a-bbd1-0b0b5feca67b)



8. For orchestration purposes Azure Data factory pipelines are used to automate the databricks notebooks that trigger every day to refresh and reload the data for the new races.

![photo6](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/54a8702e-3754-4d5c-a35f-6bfdd712e743)
![photo7](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/067e6b87-184f-4bbd-9a09-db3e0d2f1e00)



