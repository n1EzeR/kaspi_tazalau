# About
The project cleans data collected from https://github.com/n1EzeR/kaspi_parser

## Processing steps
### Part I
1. Walks through all categories, parses each product JSON file, detects language of product's review (either RU or KZ) using https://github.com/nlacslab/kaznlp
2. Combines all data from products into one CSV file
3. Combines all categorized CSV files into one large file

### Part II
1. Removes all stopwords from reviews
2. Lemmatizes reviews using https://github.com/nlpub/pymystem3
3. Outputs new `cleaned_data.csv`

# Installation

## Prerequisites
1. Python 3, pip3
2. Sucessfully parsed data from https://github.com/n1EzeR/kaspi_parser
3. Pipenv (`pip3 install -m pipenv`)

## Installation
```
git clone https://github.com/n1EzeR/kaspi_tazalau
cd kaspi_tazalau
pipenv install
```

## Usage
```
cd code
python app.py
```
