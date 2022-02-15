# Products Review WebScrapper

## Overview 
This project implements web scrapper functionality using sallenium library which automates some of the actions and helps extracting required data. 
Here our aim is to scrap comments, reviews of a particular product searched by user and store same details in cassandra data base as well as to provide 
option to view extracted data and to download it in csv file. Storing extracted data in database provides count of comments/reviews already extracted for particular product.
So if any user ask for 10 reviews to extract for product A and we already have 5 reviews in DB then webscrapper will scrap next 5 reviwes only.


## Configuration

Inside config.ini file replace your cassandra credentials in order to connect your own cassandra db 
downloand connection bundle from cassandra datastrax and place it inside folder "CassandraDataStraxPythonBundle"
then mention same path in config.ini file -> CassandraDataStraxPythonBundle\secure-connect-test1.zip
 
