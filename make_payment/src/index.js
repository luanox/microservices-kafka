const express = require('express');
const {v4: uuidv4} = require('uuid');
const app = express();
const {Kafka} = require('kafkajs');
app.use(express.json());

const kafka = new Kafka({
  clientId: 'teste_rede',
  brokers: ['localhost:9092']
});

const consumer = kafka.consumer({groupId: 'payment'});
const producer = kafka.producer();

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({topic: 'RECEIVE_ORDER', fromBeginning: false});
  await consumer.run({
    eachMessage: async ({topic, partition, message}) => {
      let order = JSON.parse(message.value.toString())
      await makePayment(order);
    },
  });
}

const makePayment = async (order) => {
  console.log(order);
  order.status = "PAID";
  order.payment_id = uuidv4();

  await producer.connect()
  await producer.send({
    topic: 'MAKE_PAYMENT',
    messages: [
      {value: JSON.stringify(order)}
    ]
  })

  await producer.disconnect()
}

run().catch(console.error)

app.listen(3335)