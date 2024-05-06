# Desafio de Processamento de Dados de Grande Volume

O objetivo deste teste é avaliar suas habilidades em manipular e analisar grandes volumes de dados em Python, de forma eficiente e com consumo otimizado de recursos.

Você receberá um arquivo CSV de grande porte, denominado 'vendas_grandes.csv', com cerca de 5GB. O arquivo contém dados de vendas de uma cadeia de varejo. As colunas incluem: Order Date, Produto, Quantidade, Preço_Unitário, e Loja.

Instruções:
1. Implemente uma solução para ler o arquivo 'vendas_grandes.csv' de forma eficiente, considerando o grande volume de dados.
2. Identifique o produto mais vendido em termos de quantidade e canal.
3. Determine qual pais e região teve o maior volume de vendas (em valor).
4. Calcule a média de vendas mensais por produto, considerando o período dos dados disponíveis.

Requisitos:
- Sua solução deve ser capaz de rodar em uma máquina com memória limitada, não assuma que o arquivo inteiro pode ser carregado na memória de uma vez.
- Use técnicas como leitura em partes (chunking), processamento em fluxo (streaming) ou ferramentas específicas para grandes volumes de dados.
- Priorize a eficiência do processamento.
- O uso de bibliotecas como Pandas é permitido, especialmente com seu recurso de leitura em chunks.
- Documente seu código adequadamente e inclua comentários explicativos sobre suas escolhas de implementação.

Entrega:
- Um script Python (.py) que realiza as tarefas acima.
- Um relatório em texto explicando como sua solução aborda o problema de grandes volumes de dados, incluindo qualquer otimização de performance que você tenha implementado.

Dicas:
- Explore a função `read_csv` do Pandas com o parâmetro `chunksize` para processar o arquivo em partes.
- Considere o uso de estruturas de dados eficientes para armazenamento temporário de informações durante o processamento.
- Avalie o consumo de memória e tempo de execução para garantir que sua solução seja realmente eficiente.

Boa sorte!
# Nota: Este desafio é projetado para testar a habilidade do candidato em lidar com análises de dados em grande escala, focando em eficiência e otimização de recursos.
