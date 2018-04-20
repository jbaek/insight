# Jason Baek 
Insight Data Engineering 18B

## Project Idea
- Improve reaction to positive West Nile Virus tests combining environmental sensor and image data

## Purpose and Use Cases

West Nile Virus (WNV) is a mosquito borne disease that can be fatal with 262 deaths last year. Mosquitoes breed in standing water or damp environments. Local pest control agencies regularly test for WNV and can react to positive tests and bird deaths by finding and addressing mosquito breeding grounds. However, mosquitoes can travel up to two miles, so by providing relevant sensor and image data associated with positive tests for WNV, pest control agencies can more effectively deploy people to find and evaluate mosquito breeding locations to address them. In the future, the pipeline can be used to use the ingested data to monitor other situations like flooding in remote or urban locations where people need to be deployed to address the situation. 

### Inputs
- WNV testing data (Chicago)
- Humidity and soil moisture and sensor data (Chicago)
- Wind data?
- Geotagged and hashtagged images of standing water
- (Optional) Animal death reports

### Outputs
- Map of locations to check for mosquito breeding 
- Dashboard to display map and relevant input data

## Proposed Architecture
- Ingest data: Streaming Framework (Kafka) 
- Combine data of sensor data near positive tests 
- Compute probabilities of mosquito breeding
- Translate to map locations
- Store input and results: Cassandra  
- Output results in web app: Flask

## References
- http://westnile.ca.gov/
- https://data.cityofchicago.org