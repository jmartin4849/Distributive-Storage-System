echo $HOST $PORT $NAME $ZK_ADDR $ZK_PORT
ssh -n $HOST nohup java -jar ~/ece419/ece419-project/m2/m2-server.jar $PORT $NAME $ZK_ADDR $ZK_PORT
