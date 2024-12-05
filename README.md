# Project for the Module: Project - Data Engineering

Contains the complete project for the batch processing in data-intensive applications


## Setup Guide of the Project


### 0. Ensure Python and Docker-Desktop are installed


### 1. Move to path where the project shall be cloned and execute:
  

    git clone https://github.com/Logrimm99/Project-Data-Engineering.git


#### 1.5 Get the dataset from
  https://www.kaggle.com/datasets/bwandowando/2-million-formerly-twitter-google-reviews?select=TWITTER_REVIEWS.csv

#### Copy it into the producer folder, make sure the path is as follows (from the main directory of the project)
  producer/TWITTER_REVIEWS.csv


### 2. To run the pipeline:

#### 2.1 Move to the main directory of the project

#### 2.2 Execute
    
    docker-compose up --build


### 3. To access the dashboard

#### 3.1 Open http://localhost:8080/ in a browser

#### 3.2 Login using the following credentials:
    Email: admin@admin.com
    Password: admin

#### 3.3 Expand Servers
#####  3.3.1 If a second password is required for the database: 
    password: password

#### 3.4. Right-click on the mydb database and click on query tool

#### 3.5. Perform the following query to list everything from the table containing the aggregated data:
    SELECT * FROM aggregated_data;


#### 4. To stop the project:

##### 4.1 Open the terminal where it is running

##### 4.2 Press Ctrl + C

##### 4.3 To shut down the containers, enter
    docker-compose down
