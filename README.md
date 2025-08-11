# ETL_Pipeline_DataFlow

 🚀 Load CSV from GCS to BigQuery - Airflow Pipeline

![Sparkify Data Model](/Images/workflow.png)    


## 📝 Description
Pipeline Airflow automatisé pour :
- Charger des fichiers CSV/JSON depuis Google Cloud Storage (GCS)
- Créer des tables BigQuery avec validation de schéma
- Générer des vues analytiques
- Gérer les erreurs et logs

## 🛠 Technologies
- **Google Cloud Platform** (GCS, BigQuery)
- **Apache Airflow 2.6+**
- **Python 3.10+**

## 📦 Installation

## Structure de code 
1. Initialisation du DAG
 ```python

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator


from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

```
2. Définition du DAG Principal

```js

function transform(line){
    // 1. Clean the line and split into values
         const cleanedLine = line.trim().replace(/[\t\"']/g, '');

    var values = cleanedLine.split(","); // split the input by commas (CSV format)

    if (values[0] === 'Country' || values.length < 8) {
        return null; // ski processing the header row
    }
    // create an object mapping the values to the BigQuery schema 
    const obj = {
        Country :values[0],
        Year : convertTovalidYear(values[1]),
        co2_gigagrams: safeParseFloat(values[2]), 
        hfc_gigagrams: safeParseFloat(values[3]), 
        methane_gigagrams: safeParseFloat(values[4]), 
        pfc_gigagrams:  safeParseFloat(values[5]), 
        sf6_gigagrams : safeParseFloat(values[6]), 
        n2o_gigagrams: safeParseFloat(values[7]) 

    }; 
    // Additional validatoion
    if (!obj.Country || obj.Country.length === 0) {

            return null;  // skip row with missing country 
    }
    return JSON.stringify(obj); 
}


function safeParseFloat(value) {
        if (!value) return 0; 
        const num = parseFloat(value.trim()); 

        return isNaN(num)? 0: num; 
}

// Help function to validate and convert year

function convertTovalidYear(yearValue) {

        if (!yearValue) return null;     

        // Le cas ou la contrue apparait 

        const cleaned = yearValue.trim(); 
        if (/^[a-zA-Z]/.test(cleaned)) {
            return null; 
        }

        const year = parseInt(cleaned); 
        return (year >= 1900 && year <= 2100) ? year : null; 
}

``
