## producer service 
publish order events to kafka 

>>docker compose up -d
 to start all the services enlisted in docker compose file 

// temporarily :
service running on local host 8080 , 
all request directed toward localhost 8080 : api endpoint http://localhost:8080/createorder 

// request to endpoint 
>>  curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer":"Alice","product":"Mechanical Keyboard","quantity":1,"price":89.99}'

or in my case : set up a api document in postman with a post request 

// to visuliaze order stack in kafka message broker 
// using docker exex 
                
>> docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders --from-beginning

