#!/bin/bash
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.2.4.tar.gz
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.2.4.tar.gz.sha512
shasum -a 512 -c elasticsearch-6.2.4.tar.gz.sha512 
tar -xzf elasticsearch-6.2.4.tar.gz
cd elasticsearch-6.2.4/
