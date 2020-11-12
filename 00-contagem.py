from mrjob.job import MRJob
from mrjob.step import MRStep

class ContadorFrequencias(MRJob):
    def mapper(self, _, linha):
        palavras = linha.split()
        for palavra in palavras:
            yield palavra, 1

    def mapper_tratamento(self, chave, contagem):
        ultimo = chave[-1]
        if ultimo in [",", "!", ":"]:
            yield chave[0:-1], contagem
        else: 
            yield chave, contagem

    def reducer(self, chave, valores):
        yield chave, sum(valores)

    def steps(self):
        return [
            MRStep(mapper=self.mapper),
            MRStep(mapper=self.mapper_tratamento,
                   reducer=self.reducer),
            
        ]

if __name__ == '__main__':
    ContadorFrequencias.run()