# AdTech

![AdTech Image](images/ad-tech.jpg)


**AdTech** is a system designed to generate product recommendations for advertisers based on user interaction data. The project processes two types of input logs:

1. **Product View Logs**  
   Each entry represents a user viewing a product on an advertiser's website. These logs include:
   - Advertiser ID  
   - Product ID  
   - Date of the view  

2. **Ad Interaction Logs**  
   Each entry represents a user interacting with an ad on any website. These logs include:
   - Advertiser ID  
   - Product ID  
   - Type of interaction (impression or click)  
   - Date of the interaction  

Using this data, the system calculates two types of recommendations:  

- **Top Products**: Recommends the most viewed products on the advertiser's website.  
- **Top CTR Products**: Recommends products with the best ad performance, measured by Click-Through-Rate (CTR).  

## System Overview

- **Cloud Infrastructure**: Built on AWS.  
- **Daily Pipeline**: Data is processed through an Airflow pipeline, which generates daily recommendations.  
- **Database**: Results are stored in a PostgreSQL database hosted on AWS RDS.  
- **API Access**: A REST API allows querying recommendations for specific advertisers, models, and dates. Additional endpoints enable interaction with the stored data.  

The pipeline ensures up-to-date recommendations, making it easy to dynamically retrieve insights and drive advertising decisions.

# Project Setup Instructions

## Initial Setup

Follow these steps to set up the project environment for the first time:

1. **Clone the repository**:

    ```sh
    git clone <repository_url>
    cd <repository_directory>
    ```

2. **Create the conda environment**:

    ```sh
    conda create --name AdTech python=3.9
    ```

3. **Activate the environment**:

    ```sh
    conda activate AdTech
    ```

4. **Install the packages from `requirements.txt`**:

    ```sh
    pip install -r requirements.txt
    ```
