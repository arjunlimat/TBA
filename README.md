# Project Name
Project Description: This project  developed on Django and deployed on the AWS cloud and performs matching and mismatch analysis on participant values. It fetches data from both a file and an API to compare and analyze the values.

## Features

- Read participant values from a file and make an API call.
- Compare different values from file with API response
- Performs matching and mismatch analysis on the fetched values.
- Generates reports or visualizations based on the analysis results.
- Provides a GUI/API for interacting with the project and accessing the analysis results.
- Test cases written inside file Validation/test.py

## Technologies Used

- Django: Python web framework for building the project.
- AWS: Cloud platform used for deployment.
- File Handling: Reads participant values from a file.
- API Integration: Fetches data from an external API.
- Data Analysis: Compares and analyzes participant values for matching and mismatch detection.
- Reporting/Visualization: Generates reports or visualizations to present the analysis results.

## Deployment

The project is deployed on the AWS cloud platform. Here are the steps to deploy the project:

1. Set up an AWS account and create an EC2 instance.
2. Install the required dependencies (Python, Django, etc.) on the EC2 instance.
3. Clone the project repository onto the EC2 instance.
4. Configure the project settings (database, API credentials, etc.).
5. Start the Django development server or deploy using a WSGI server like Gunicorn.
6. Set up the necessary security groups, load balancers, and domain mapping as per your requirements.
7. Access the deployed project using the provided URL or domain.

## Usage

1. Upload the file containing participant values to the project.
2. Use the provided GUI/API endpoints to initiate the matching and mismatch analysis.
3. Monitor the progress and wait for the analysis to complete.
4. Access the generated reports or visualizations to view the analysis results.
5. Optionally, customize the analysis parameters, reporting formats, or other settings as required.


