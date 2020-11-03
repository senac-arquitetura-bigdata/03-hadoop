from mrjob.job import MRJob

class ContadorFrequencias(MRJob):
    def mapper(self, _, linha):
        yield "caracteres", len(linha)
        yield "palavras", len(linha.split())
        yield "linhas", 1

    def reducer(self, chave, valores):
        yield chave, sum(valores)

if __name__ == '__main__':
    ContadorFrequencias.run()