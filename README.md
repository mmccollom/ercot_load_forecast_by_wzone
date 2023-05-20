# ERCOT Load Forecast by Weather Zone Scrape
Electric Reliability Council of Texas (ERCOT) releases a weather zone and system-wide load forecast every hour for the next 7 days. This lambda function scrapes the ERCOT website for the latest load forecast and saves it to an S3 bucket in parquet format. The lambda function is triggered by a CloudWatch event every hour.

### Getting Started
Simply adjust the s3_path variable in main.py to point to your S3 folder.

### Deploying
This project has been set up to be deployed as a serverless application via SAM. To deploy the application, run the following command (AWS account and AWS SAM CLI required):
```
sam build
sam deploy --guided
```

### License
This project is licensed under the MIT License