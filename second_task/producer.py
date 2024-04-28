import pika
import faker
from mongoengine import connect
from email_model import Contact

connect(db='Email', host='127.0.0.1', port=27017)

credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost', port=5672, credentials=credentials))
channel = connection.channel()

channel.exchange_declare(exchange='task_mock', exchange_type='direct')
channel.queue_declare(queue='task_queue', durable=True)
channel.queue_bind(exchange='task_mock', queue='task_queue')


def generate_fake_data():
    fake_data = faker.Faker()
    num_contact = 7

    for i in range(num_contact):
        contact_data = {
            'fullname': fake_data.name(),
            'email': fake_data.email()
        }
        contact = Contact(**contact_data)
        contact.save()


def main():
    contacts = Contact.objects()
    for contact in contacts:
        channel.basic_publish(
            exchange='task_mock',
            routing_key='task_queue',
            body=f'{contact.id}'.encode(),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))
        print(f" [x] Sent client id {contact.id}")
    connection.close()
    
    
if __name__ == '__main__':
    # generate_fake_data()
    main()

