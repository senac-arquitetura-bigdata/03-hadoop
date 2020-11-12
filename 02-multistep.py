from mrjob.job import MRJob
from mrjob.step import MRStep

class ContadorFrequencias(MRJob):
    def mapper(self, _, linha):
        yield "palavras-linhas", {'palavras': len(linha.split()), 'linhas': 1}

    def reducer(self, chave, valores):
        palavras = 0
        linhas = 0
        for valor in valores:
            palavras += valor['palavras']
            linhas += valor['linhas']

        yield chave, [palavras, linhas]

    def reducer_media(self, chave, valores):
        for valor in valores:
            yield "media", valor[0] / valor[1] # cálculo da média

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_media)
        ]

if __name__ == '__main__':
    ContadorFrequencias.run()