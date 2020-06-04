# About
The project cleans data collected from https://github.com/zhanymkanov/marketplace_parser

## Parsed data can be downloaded here:
https://github.com/zhanymkanov/reviews_dataset
- Partially cleaned (both row and cleaned versioans are available though)
- ~190k rows

# Processing steps
### Part I
1. Walks through all categories, parses each product JSON file, detects language of product's review (either RU or KZ) using https://github.com/nlacslab/kaznlp
2. Combines all data from products into one CSV file per category
3. Combines all categorized CSV files into one large `all.csv` file

### Part II
1. Removes all stopwords from `all.csv` reviews
2. Lemmatizes reviews using https://github.com/nlpub/pymystem3
3. Outputs new `cleaned_data.csv`

# Installation

## Prerequisites
1. Python 3
2. Docker, docker-compose
3. Sucessfully parsed data from https://github.com/zhanymkanov/marketplace_parser
4. Make sure data is collected from parser and stored in proper directories locally

## Installation
1. Clone the project
```
git clone https://github.com/zhanymkanov/reviews_tazalau
```
2. Go to project directory
```
cd reviews_tazalau
```
3. Set up the docker container
```
docker-compose build
```

## Usage
Run the `main.py`
