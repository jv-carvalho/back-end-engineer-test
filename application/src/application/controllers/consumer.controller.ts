import { Controller, Logger } from '@nestjs/common';
import { MessagePattern, Payload } from '@nestjs/microservices';
import { HttpService } from '@nestjs/axios';
import { map } from 'rxjs/operators';
import { MongooseModule } from '@nestjs/mongoose';
import { State, StateSchema } from './schemas/state.schema';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as csv from 'csv-parser';
import * as fs from 'fs';

@Module({
  imports: [
      MongooseModule.forRoot('mongodb://localhost/mydatabase'),
      MongooseModule.forFeature([{ name: State.name, schema: StateSchema }]),
  ],
  controllers: [ConsumerController],
  providers: [ConsumerController, DataProcessorService],
})
export class AppModule {}

@Controller('consumer')
export class ConsumerController {
  private readonly logger = new Logger(ConsumerController.name);

  constructor(
    private httpService: HttpService,
    @InjectModel(State.name) private stateModel: Model<State>,
    ) {}

  @MessagePattern('Csv_Process')
  async receiveFromQueue(@Payload() filePath: string): Promise<string> {
    // Leitura do arquivo CSV
    const data = await this.readCsvFile(filePath);

    // Processamento dos dados
    const processedData = data.map(item => {
      // Tratamento de dados nulos
      const processedItem = { ...item };

      if (!processedItem.nome) {
        processedItem.nome = 'Nome não informado'; // Substitui nome nulo
      }

      if (!processedItem.email) {
        processedItem.email = 'email_padrao@exemplo.com'; // Substitui email nulo
      }

      // Validação básica (exemplo: verificar se a idade é um número e maior que 18)
      if (isNaN(processedItem.idade) || processedItem.idade < 18) {
        this.logger.warn(`Invalid age for item: ${JSON.stringify(item)}`);
        return null; // Ignora linha com idade inválida
      }

      return processedItem;
    }).filter(item => item !== null); // Remove itens nulos após processamento

    // Remoção de duplicados (exemplo: usando lodash)
    const uniqueResults = _.uniqBy(processedData, 'id'); // Assumindo que 'id' é um identificador único

    // Divisão em batches
    const batches = [];
    for (let i = 0; i < uniqueResults.length; i += 1000) {
      batches.push(uniqueResults.slice(i, i + 1000));
    }

    // Envio dos batches para a segunda aplicação (exemplo usando API REST)
    for (const batch of batches) {
      await this.httpService.post('http://segunda-aplicacao/api/dados', batch)
        .pipe(map((response) => {
          this.logger.log(`Batch sent successfully: ${JSON.stringify(response.data)}`);
        }))
        .toPromise()
        .catch(error => {
          this.logger.error(`Error sending batch: ${error.message}`);
        });
    }

    return `Processed ${batches.length} batches`;

  }

  async readCsvFile(filePath) {
    const results = [];
    return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => {
                resolve(results);
            })
            .on('error', (error) => {
                reject(error);
            });
    });
}

const express = require('express');
const mongoose = require('mongoose');

const app = express();
const port = 3001; // Defina a porta que desejar

// Conectar ao MongoDB
mongoose.connect('mongodb://localhost/mydatabase');

// Schema para os dados
const stateSchema = new mongoose.Schema({
    state: String,
    population: Number
});
const State = mongoose.model('State', stateSchema);

// Middleware para parsear o corpo da requisição como JSON
app.use(express.json());

// Rota para receber os dados da primeira aplicação
app.post('/api/dados', async (req, res) => {
    const data = req.body;

    // Agrupar os dados por estado e calcular a população
    const statesData = data.reduce((acc, item) => {
        const state = acc.find(s => s.state === item.state);
        if (state) {
            state.population++;
        } else {
            acc.push({ state: item.state, population: 1 });
        }
        return acc;
    }, []);

    // Salvar os dados no MongoDB
    await State.insertMany(statesData);

    res.send('Dados recebidos e armazenados com sucesso');
});

app.listen(port, () => {
    console.log(`Segunda aplicação rodando na porta ${port}`);
});
}