from mongoengine import Document, StringField, BooleanField, EmailField


class Contact(Document):
    fullname = StringField(required=True)
    email = EmailField(required=True)
    is_send = BooleanField(default=False)