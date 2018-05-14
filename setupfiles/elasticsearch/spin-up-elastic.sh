#!/bin/bash
# echo  "${0%/*}"
CLUSTER_NAME=es-cluster 

#### elasticsearch-cluster ---< defined in tags
peg up $PRJ_DIR/setupfiles/elasticsearch/master.yml & 
peg up $PRJ_DIR/setupfiles/elasticsearch/workers.yml & 

## install services 
wait

peg fetch ${CLUSTER_NAME}

wait

# supposed to not be required ...
peg install ${CLUSTER_NAME} environment
# network cluster in passwordless --> master owns
peg install ${CLUSTER_NAME} ssh
wait 
# aws keys -- check may not be needed here, 
peg install ${CLUSTER_NAME} aws #

# software 
peg install ${CLUSTER_NAME} hadoop # may as well, 
peg install ${CLUSTER_NAME} elasticsearch

### start services 
wait

# peg service ${CLUSTER_NAME} hadoop stop
# peg service ${CLUSTER_NAME} elasticsearch stop

peg service ${CLUSTER_NAME} hadoop start
peg service ${CLUSTER_NAME} elasticsearch start
