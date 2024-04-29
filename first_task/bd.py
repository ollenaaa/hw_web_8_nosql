import json
import redis
from redis_lru import RedisLRU
from mongoengine import connect
from models import Author, Quote

connect(db='AuthorsAndQuotes', host='127.0.0.1', port=27017)

client = redis.StrictRedis(host="localhost", port=6379, password=None)
cache = RedisLRU(client)


def load_authors_from_json():
    with open('../json/authors.json', 'r') as file:
        data = json.load(file)

    for item in data:
        author_data = {
                'fullname': item['fullname'],
                'born_date': item['born_date'],
                'born_location': item['born_location'],
                'description': item['description']
            }
        author = Author(**author_data)
        author.save()


def load_quotes_from_json():
    with open('../json/quotes.json', 'r') as file:
        data = json.load(file)

    for item in data:
        author_name = item['author']
        author = Author.objects(fullname=author_name).first().id

        quote_data = {
                    'text': item['quote'],
                    'author': author,
                    'tags': item['tags']
                }
        quote = Quote(**quote_data)
        quote.save()

@cache
def search_quotes_by_author(name, task):
    if task == 'main':
        author = Author.objects(fullname=name).first()
        quote = Quote.objects(author=author)
        return quote
    
    elif task == 'additional':
        authors = Author.objects(fullname__istartswith=f'{name}')
        quotes = Quote.objects(author__in=authors)
        return quotes
    
@cache
def search_quotes_by_tag(tag, task):
    if task == 'main':
        quotes = Quote.objects(tags=tag)
        return quotes
    
    elif task == 'additional':
        quotes = Quote.objects(tags__istartswith=f'{tag}')
        return quotes
    
@cache
def search_quotes_by_tags(tags):
    quotes = Quote.objects(tags__name__in=tags)
    return quotes


def main(task):
    while True:
        enter_text = input("Enter query (name:, tag:, tags:, or exit): \n").split(":")

        if enter_text[0] == 'exit':
            break
        elif enter_text[0] == 'name':
            name = enter_text[1]
            quotes = search_quotes_by_author(name, task)
        elif enter_text[0] == 'tag':
            tag = enter_text[1]
            quotes = search_quotes_by_tag(tag, task)
        elif enter_text[0] == 'tags':
            tags = enter_text[1].split(",")
            quotes = search_quotes_by_tags(tags)

        if quotes:
            for quote in quotes:
                print(quote.text)


if __name__ == "__main__":
    # load_authors_from_json()
    # load_quotes_from_json()

    #first task
    print('Main Task')
    main('main')

    #additional task
    print('Additional Task')
    main('additional')