# Zoom ETL Project

This project is implemented using Docker container to containerize the environment which run Apache Airflow to orchestrate Python scripts. Python scripts download users, meetings, and recordings data to store in PostgreSQL database and store actual recordings to AWS S3.

The pipeline downloads the list of users email addresses. Next, using that list of email addresses, it downloads user information and meetings information associated with that user email address. Using the list of meetings, it extract the meeting details, list of participants, and recordings information. Using the recording information, it downloads the recordings and upload to S3.

The pipeline downloads every users email address existing in the system. For the meetings, it only retrieves from the last time the pipeline is run.