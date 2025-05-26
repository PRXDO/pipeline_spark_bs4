from bs4 import BeautifulSoup
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import requests
import re

dadosAcumulados = [] # lista para armazenar os dados acumulados

page = 1 # variavel para controlar a paginação
i = 1

while True: 

    url = f'https://books.toscrape.com/catalogue/page-{page}.html'

    # resposta com o conteudo da pagina
    response = requests.get(url) 
    
    # verifica se a resposta é diferente de 200, se for, sai do loop
    if response.status_code != 200: 
        break

    result = response.text 

    soup = BeautifulSoup(result, 'lxml')
    
    # busca diretamenta todas as tags li que estão dentro de uma tag ol com a classe row
    for item in soup.select('ol.row li'): 
        

        # busca tags a que tenham o atributo title
        title_aux = item.find("a", attrs={"title":True}) 
        # extrair o texto que está dentro do atributo title de cada tag a encontrada 
        title = title_aux.get('title') 

        # busca tags p que tenham a classe star-rating
        star_aux = item.find("p", class_="star-rating")
        # extrair o texto que está dentro do atributo class na posicao 1 de cada tag p encontrada(é a informação que tem a avaliação do livro) 
        star = star_aux.get('class')[1]

        # busca tags p que tenham a classe price_color
        price_aux = item.find("p", class_="price_color")
        # extrair o texto que está dentro de cada tag p encontrada 
        price = price_aux.getText()

        # busca tags p que tenham a classe instock availability
        availability_aux = item.find("p", class_="instock availability")
        # extrair o texto que está dentro de cada tag p encontrada
        availability = availability_aux.getText()

        # Passa todas as informações encontradas como um dicionario para a varivavel dadosAcumulados, contruindo uma lista de dicionarios
        dadosAcumulados.append({'title':title, 'rating':star, 'price':price, 'availability':availability})

        i = i + 1
        print("loop for: ",i)

    page = page + 1 

    print("pagina: ",page)


# Para cada item dentro da lista dadosAcumulados, crie um novo dicionário com apenas duas informações
starTable = [{"title": item["title"], "stars": item["rating"]} for item in dadosAcumulados]
priceTable = [{"title": item["title"], "price": item["price"]} for item in dadosAcumulados]
availabilityTable = [{"title": item["title"], "disponibility": item["availability"]} for item in dadosAcumulados]


