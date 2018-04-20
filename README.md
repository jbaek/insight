# Jason Baek 
Insight Data Engineering 18B

## Project Idea
- Improve reaction to positive West Nile Virus tests with environmental and image data to find potential mosquito breeding grounds

## Purpose and Use Cases

West Nile Virus (WNV) is a mosquito borne disease that can be fatal with 262 deaths in the US last year. Mosquitoes breed in standing water or damp environments. Local pest control agencies regularly test for WNV and react to positive tests and bird deaths. However, mosquitoes can travel up to two miles and infected people become transmitters of the disease, so when positive WNV results occur, mosquito control may not be targeted. Instead, suggestions of potential mosquito breeding grounds can be precomputed from sensor and image data searching for standing water or warm, damp environments. When positive tests occur, these suggestions can be reported to pest control agencies to effectively deploy people to evaluate mosquito breeding locations and address them. The goal is to design the pipeline to scale up to larger areas. Also the pipeline can be used for other situations like wildfires or flooding where people need to be quickly and accurately deployed to address the situation. 

### Inputs
- WNV testing data (Chicago) and animal death reports
- Humidity, temperature, and soil moisture sensor data (Chicago)
- Rainfall (Weather Service)
- Geotagged and hashtagged images of standing water (Flickr?)

### Outputs
- Recommended locations to check for mosquito breeding 
- Dashboard to display map of locations and relevant input data

## Proposed Architecture
- Store sensor data: S3
- Store image metadata: Postgres
- Ingest data: Kafka 
- Compute probabilities of mosquito breeding: Spark
- Store results: Postgres 
- Output map in web app: Flask

## References
- http://westnile.ca.gov/
- https://data.cityofchicago.org