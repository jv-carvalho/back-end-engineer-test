# Desafio Engenheiro Back-End Pleno 👩‍💻

Agradecemos seu interesse em se tornar parte da nossa equipe!

## Sobre a Empresa e o Ambiente de Trabalho 🚀

Somos a **Growth Digital Marketing**, uma empresa especializada em marketing digital focada no mercado de **iGaming**. No nosso time de gestão de tráfego, nossos gestores investem mais de meio milhão de reais por dia em tráfego pago. Estamos em plena expansão, e nossa equipe é composta pelos melhores profissionais em suas áreas. Se você for escolhido, é porque acreditamos que você também é um dos melhores na sua especialidade.

Nossa empresa é altamente competitiva, e buscamos a excelência em cada projeto. Valorizamos a entrega pontual e a alta qualidade e mantemos um ambiente colaborativo onde todos se ajudam sempre que possível. Apoiamos o crescimento coletivo, equilibrando a intensidade de um mercado competitivo com uma cultura leve e de cooperação.

## Sobre o Ambiente de Trabalho no Departamento de T.I ☕

O departamento de TI da GDM reflete a cultura colaborativa da empresa. Somos um time de profissionais altamente qualificados com expertise em tecnologias de ponta e uma disposição para compartilhar conhecimento. O nosso setor é composto por engenheiros de frontend e backend que trabalham com tecnologias avançadas, além de um arquiteto de software que lidera a definição das melhores práticas e da infraestrutura, especialmente para soluções escaláveis em iGaming.

Nosso front-end é desenvolvido com **React.js** e **Next.js**, garantindo interfaces de alta performance. No backend, utilizamos principalmente **Node.js** e **NestJS** junto com uma robusta infraestrutura em nuvem na **AWS**.

## Stack Necessária 💻

- NodeJS ✔
- TypeScript ✔
- Nest.JS ✔
- Docker ✔
- MongoDb ✔
- RabbitMQ ✔

## Requisitos Técnicos 😁

- Ambas as aplicações devem ser desenvolvidas em NestJS, seguindo os padrões de arquitetura DDD e aplicando os princípios de SOLID.
- Cada aplicação deve incluir uma imagem Docker e um Dockerfile para facilitar o empacotamento e a execução em containers.
- Se desejar, pode incluir um docker-compose para cada uma das aplicações, o que facilitará a simulação do deploy na AWS.
- A comunicação entre as duas aplicações pode ser feita via HTTP, gRPC, ou através de algum sistema de mensageria (como RabbitMQ ou Kafka), à sua escolha, desde que seja justificada e eficiente para o envio em batches.

## Descrição do Desafio 📰

Se você pensa que o papel de um Engenheiro Back-End se resume a criar C.R.U.D. e consumir APIs, está muito enganado. Estamos procurando alguém que vá além, com habilidade de pensar sistemicamente e atuar de forma crítica em todo o ciclo de desenvolvimento de soluções robustas, escaláveis e orientadas a resultados.

### Objetivo do Projeto

O objetivo deste projeto é criar uma aplicação robusta para processamento de dados em batch, dividida em duas partes: uma aplicação que realiza a leitura e o envio dos dados em lotes e outra aplicação para recepção e armazenamento eficiente dessas informações, agregando-as por estado e total de pessoas, com a possibilidade de deploy em um ambiente escalável como a **AWS**.

Você encontrará em nosso repositório um arquivo .csv contendo 10 mil linhas e 4 colunas. Seu desafio como engenheiro back-end será desenvolver duas aplicações utilizando NestJS, aplicando os princípios de DDD (Domain-Driven Design) e SOLID, para processar, tratar e armazenar esses dados.

### Aplicação 1 - Leitura e Envio dos Dados:

O seu papel é desenvolver uma aplicação que seja capaz de:

- Ler o arquivo .csv com eficiência, você pode optar por criar uma rota no controlador para receber o arquivo ou simplesmente ler o arquivo embutido no código, fica ao seu critério.
- Processar os dados, tratando qualquer inconsistência.
- Dividir os dados em batches de 1000 registros, respeitando o limite de envio de 1000 dados por segundo para a segunda aplicação.
- Enviar esses batches para a segunda aplicação por meio de uma interface de comunicação (como uma API REST ou mensageria).

### Aplicação 2 - Recepção e Armazenamento dos Dados:

**A segunda aplicação já possui parte do código implementado, mas precisa ser completada.**

- Esta aplicação será responsável por:
- Receber os batches de dados da primeira aplicação.
- Processar e armazenar os dados recebidos em um banco de dados não relacional, preferencialmente MongoDB, utilize a ODM que melhor agradar.
- O dado que será armazenado no banco é o nome dos estados presentes no arquivo .csv, juntamente com a quantidade total de pessoas para cada estado (i.e., um somatório de pessoas por estado).

### Etapas:

1. Leitura de Dados (Aplicação 1):
   
- Ler o arquivo .csv com 10 mil linhas de maneira eficiente, evitando carregar o arquivo inteiro na memória de uma vez.
- Processar os dados para garantir sua integridade (tratar dados nulos, duplicados, etc.).
- Implementar uma lógica de envio dos dados em batches de 1000 registros por segundo para a segunda aplicação.

2. Armazenamento e Processamento (Aplicação 2):

- Completar a aplicação para que ela possa receber os dados enviados pela primeira aplicação em batches.
- Armazenar as informações no MongoDB, salvando a quantidade total de pessoas por estado.
- Garantir que a aplicação seja capaz de lidar com grande volume de dados sem perda ou duplicidade.

3. Deploy e Execução:

- Criar Dockerfiles para ambas as aplicações, garantindo que elas possam ser empacotadas e executadas em containers de forma isolada.
- Opcionalmente, criar um docker-compose para simular o ambiente de produção.
- Certifique-se de que a solução seja escalável e eficiente para um possível deploy na AWS.

### Considerações:

**A segunda aplicação é esta que contem o arquivo de instruções, o CSV, você encontra na pasta support**

A solução deve ser testável e escalável, garantindo que possa suportar o envio contínuo de dados em batch.
Utilize boas práticas de desenvolvimento, como testes unitários, tratamento de erros, e logs para monitorar o comportamento das aplicações.
Documente a solução para facilitar a compreensão e o deploy por outros desenvolvedores ou engenheiros.

## Diferenciais 💖

- Experiência com sistemas de mensageria como RabbitMQ ou Kafka.
- Conhecimento em práticas de CI/CD.
- Familiaridade com deploy em ambientes de produção na AWS.

## Critérios de Avaliação 📊

- Qualidade do código, organização e aderência aos princípios de DDD e SOLID.
- Eficiência na leitura e processamento dos dados do arquivo .csv.
- Capacidade de enviar os dados em batches respeitando o limite de 1000 registros por segundo.
- Implementação do armazenamento eficiente dos dados no banco de dados MongoDB.
- Uso adequado de containers Docker e, se aplicável, docker-compose.

## Próximos Passos

Envie o link da aplicação e do repositório para nosso time de recrutamento em [vagas@gdigitalmkt.com].

**Boa sorte e mãos à obra!**
