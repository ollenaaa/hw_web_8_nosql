from mongoengine import Document, EmbeddedDocument, StringField, ReferenceField, ListField


class Author(Document):
    fullname = StringField(required=True)
    born_date = StringField(required=True)
    born_location = StringField(required=True)
    description = StringField(required=True)


class Quote(Document):
    text = StringField(required=True)
    author = ReferenceField(Author, required=True)
    tags = ListField(StringField())