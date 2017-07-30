# Project Title

Nulogy entity resolution code assignment.

## Steps taken to resolve the entities

* Load given data sets in Apache Spark dataframes
* Join two datasets on title and year columns. Create an additional column with fuzzy search score on "authors". (Gives a new dataframe)
* Filter new dataframe to collect entities with fuzzy search score of more than 80. (Gives relevant resolved entities)
* Write output to new CSV

## Assumptions made to solve the problem

Title and year data is consistent across two data sets and can be used for initial filtering

## Libraries or packages used

* Apache Spark (Python)
* Python fuzzywuzzy library

## Environment Setup

* [Spark 2.2](https://d3kbcqa49mib13.cloudfront.net/spark-2.2.0-bin-hadoop2.7.tgz) - Spark Setup

```
pip install fuzzywuzzy
pip install fuzzywuzzy[speedup]

```
## Author

* **Rohan Somvanshi**
