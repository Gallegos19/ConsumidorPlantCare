import * as amqp from 'amqplib';
import fetch from 'isomorphic-fetch';

async function connect() {
    const retryInterval = 5000; // Intervalo en milisegundos para reintentar

    while (true) {
        try {
            const connection = await amqp.connect('amqp://plantcare-rabbit.integrador.xyz/');
            const channel = await connection.createChannel();

            // Declaramos la cola desde la que vamos a consumir
            const queueName = 'sensores';
            await channel.assertQueue(queueName, {
                durable: true,
                arguments: {
                    'x-queue-type': 'quorum'  // Especifica el tipo de cola como 'quorum'
                }
            });

            console.log(`Esperando mensajes en la cola ${queueName}...`);

            // Consumimos los mensajes de la cola
            channel.consume(queueName, async (msg) => {
                if (msg !== null) {
                    try {
                        // Aquí podrías procesar el mensaje según tus necesidades
                        console.log("Mensaje recibido:", msg.content.toString());

                        // Parsear el mensaje
                        const parsedContent = JSON.parse(msg.content.toString());

                        // Crear el cuerpo del mensaje según la estructura especificada
                        const data = {
                            humidity_earth: parsedContent.soil_moisture,
                            humidity_environment: parsedContent.humidity,
                            mq135: parsedContent.mq135_2,
                            brightness: parsedContent.ldr,
                            ambient_temperature: parsedContent.temperature,
                            estado: 0, // Suponiendo que 'estado' es un valor fijo o determinado por otra lógica
                            mac: parsedContent.mac_address,
                        };

                        // Enviar el mensaje a una ruta específica
                        await enviarMensaje('http://44.197.7.97:8081/api/plantRecord', data);

                        // Confirmar que hemos procesado el mensaje
                        channel.ack(msg);
                    } catch (error) {
                        console.error("Error al procesar el mensaje:", error);
                        // Rechazar el mensaje y devolverlo a la cola
                        channel.nack(msg);
                    }
                }
            });
            break; // Salir del bucle si la conexión es exitosa
        } catch (error) {
            console.error('Error al conectar con RabbitMQ:', error);
            console.log(`Reintentando en ${retryInterval / 1000} segundos...`);
            await new Promise(resolve => setTimeout(resolve, retryInterval));
        }
    }
}

// Función para enviar un mensaje a una ruta específica
async function enviarMensaje(url: string, data: any) {
    const headers: { [key: string]: string } = {
        'Content-Type': 'application/json'
    };
    console.log(data);
    const body = JSON.stringify(data);

    const options: { method: string, headers: any, body: string } = {
        method: 'POST',
        headers,
        body
    };

    try {
        const response = await fetch(url, options);
        const responseBody = await response.text(); // Lee el cuerpo de la respuesta

        if (response.ok) {
            console.log("Mensaje enviado correctamente.");
        } else {
            console.error(`Error al enviar el mensaje: ${response.statusText}`);
            console.error(`Cuerpo de la respuesta: ${responseBody}`);
        }
    } catch (error: any) {
        console.error(`Error al enviar el mensaje: ${error.message}`);
    }
}

connect();
