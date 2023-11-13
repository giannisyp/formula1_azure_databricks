# Formula1 project with Azure Databricks

## This is a project that uses azure databricks to do analysis on formula1 drivers results and teams.

![image](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/d77fe7c0-0f19-4e16-b906-2010ae189d3f)

1. In this project we ingest data from ergast-api (https://ergast.com/mrd/) that documents all races from formula1 since 1950
2. Save the data to azure datalake storage 
3. Transform the data using databricks notebooks to make the data ready for presentation 
4. Present datasets with race results , driver standings , team standings , etc ...

Architecture Used is the common medallion architecture.
A medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables). Medallion architectures are sometimes also referred to as "multi-hop" architectures.

5. Later on the project the datalake is transformed into the new delta lake architecture for even better data management.

![image](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/98f29abc-b432-40b9-866d-887eb582c5f1)


6. A couple of visualizations are made in the databricks notebooks of the dominant drivers and teams for a better understanding of the subjects that we explore.

![image](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/98016c83-9fdf-4de2-926d-1dc82eac95be)

![image](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/6fbd3098-ff54-4c7f-ac7f-7da7f99b3b7c)


7. For orchestration purposes Azure Data factory pipelines are used to automate the databricks notebooks that trigger every day to refresh and reload the data for the new races.

![image](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/2a6ae404-ddea-4646-9e9d-f5791be02142)

![image](https://github.com/giannisyp/formula1_azure_databricks/assets/119696474/eb2abc54-7650-46c5-a0c3-7e44f1400110)


