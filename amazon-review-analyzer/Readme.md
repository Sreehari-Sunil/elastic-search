# Amazon review analyzer

This scripts used for storing bulk amount of review datas into elastic search index and analyze those datas for getting
1. Total number of records
2. Top ten reviewers and their count of reviews
3. Top ten products which had reviews, their count of reviews and average rating

## Prerequisites

Ensure you have Python 3 installed on your system.

## Setup Instructions

1. **Create a Virtual Environment**  
   Run the following command to create a virtual environment:
   ```sh
   python3 -m venv venv
   ```

2. **Activate the Virtual Environment**  
   On macOS and Linux:
   ```sh
   source ./venv/bin/activate
   ```
   On Windows:
   ```sh
   venv\Scripts\activate
   ```
3. **Run the elastic search server**
    Default port will be 'http://localhost:9200'

4. **Install Required Packages**  
   Install the necessary dependencies from `requirements.txt`:
   ```sh
   pip install -r requirements.txt
   ```

## Running the Script

Execute the script file 'load_data.py' for storing 'review.json' into index using:
```sh
python3 load_data.py
```

Execute the script file 'analyze_review.py' for analysing datas for given conditions using:
```sh
python3 analyze_review.py
```

## Output

The results will be shows in terminal.


