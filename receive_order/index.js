const express = require('express');
const {v4: uuidv4} = require('uuid');
const bodyParser = require('body-parser');


const app = express();
const {Kafka} = require('kafkajs');
app.use(express.json());

const kafka = new Kafka({
  clientId: 'teste_rede',
  brokers: ['localhost:9092']
})

// Middleware


// Routes
app.post("/receive_order",async (req, res) => {
  const order = req.body;
  order.id = uuidv4();
  order.status = "PENDENTE_PAGAMENTO";

  const producer = kafka.producer()

  await producer.connect()
  await producer.send({
    topic: 'RECEIVE_ORDER',
    messages: [
      {value: JSON.stringify(order)}
    ]
  })

  await producer.disconnect()

  return res.status(201).json(order);
})

app.listen(3334)