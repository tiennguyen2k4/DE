def get_data():
    import requests
    import json
    url='https://randomuser.me/api/'
    response=requests.get(url)
    re=response.json()
    data=re['results'][0]
    return data

def change_data():
    re=get_data()
    data={}
    location=re['location']
    data['first_name']=re['name']['first']
    data['last_name']=re['name']['last']
    data['gender']=re['gender']
    data['country']=location['country']
    data['city']=location['city']
    data['address']=f"{str(location['street']['number'])} {location['street']['name']} {location['state']} {location['postcode']}"
    data['email']=re['email']
    data['username']=re['login']['username']
    data['age']=re['dob']['age']
    data['registered']=re['registered']['date']
    data['phone']=re['phone']
    return data

def send_data_producer():
    from kafka import KafkaProducer
    import time
    import json
    
    KAFKA_BROKER='localhost:9092'
    KAFKA_TOPIC='usekafka2'
    count=0
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
    
    while count < 30:
        re = change_data()
        data = json.dumps(re)
        producer.send(KAFKA_TOPIC, value=data.encode('utf-8'))
        producer.flush()
        time.sleep(1)
        count += 1
    
    producer.close()

if __name__ == "__main__":
    send_data_producer()