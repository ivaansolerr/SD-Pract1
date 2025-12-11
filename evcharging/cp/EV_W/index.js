const express = require("express"); 
const https = require('https'); 
const bodyParser = require("body-parser"); 
const fs = require("fs");
const axios = require("axios"); 
const httpsAgent = new https.Agent({
    rejectUnauthorized: false // Esto permite conectar con tu API local aunque el certificado sea autofirmado
});

const appSD = express();
appSD.use(bodyParser.json()); 

const port = 4000; 
const FILE_CIUDADES = "ciudades.txt";
const FILE_API = "api.txt";

let historialTemperaturas = {};
let estadosPrevios = {};

setInterval(() => {
    fs.readFile(FILE_API, 'utf8', (err, apiKeyData) => {
        if (err) {
            console.error(`Error: No se pudo leer ${FILE_API}. ¿Existe el archivo?`);
            return;
        }

        const apiKey = apiKeyData.trim();

        if (!apiKey) {
            console.log("El archivo api.txt está vacío.");
            return;
        }

        leerCiudadesYConsultar(apiKey);
    });

}, 4000); 


function leerCiudadesYConsultar(apiKey) {
    fs.readFile(FILE_CIUDADES, 'utf8', (err, data) => {
        if (err) {
            console.error(`Error leyendo ${FILE_CIUDADES}:`, err.message);
            return;
        }

        const lineas = data.split('\n');

        lineas.forEach((linea) => {
            linea = linea.trim();
            if (!linea) return; 
            const partes = linea.split(' '); 

            if (partes.length >= 2) {
                const cp = partes[0]; 
                const ciudad = partes.slice(1).join(' '); 

                obtenerTemperatura(ciudad, cp, apiKey);
            }
        });
    });
}

async function obtenerTemperatura(ciudad, cp, apiKey) {
    try {
        const url = `https://api.openweathermap.org/data/2.5/weather?q=${ciudad}&appid=${apiKey}&units=metric`;
        
        const response = await axios.get(url);
        const tempActual = response.data.main.temp;

        if (historialTemperaturas.hasOwnProperty(cp)) {
            const tempAnterior = historialTemperaturas[cp];
            const apiCentralUrl = `https://127.0.0.1:5000/changeState/${cp}`;
            const apiCentralGetCP = `https://127.0.0.1:5000/cps/${cp}`;
            
            if (tempAnterior >= 0 && tempActual < 0) {
                console.log(`El CP ${cp} deja de estar disponible, temperatura < 0°C`);
                try {

                    const currentStateResponse = await axios.get(apiCentralGetCP, { httpsAgent: httpsAgent });
                    const estadoOriginal = currentStateResponse.data.state; 

                    estadosPrevios[cp] = estadoOriginal;
                    console.log(`-> Estado previo guardado: ${estadoOriginal}`);
                    
                    await axios.put(
                        apiCentralUrl, 
                        { state: "FUERA DE SERVICIO" }, 
                        { httpsAgent: httpsAgent }
                    );
                    console.log(`-> Estado de ${cp} forzado a FUERA DE SERVICIO.`);

                } catch (err) {
                    console.error(`Error gestionando congelación de ${cp}:`, err.message);
                }
            }
            else if (tempAnterior < 0 && tempActual >= 0) {
                console.log(`El CP ${cp} vuelve a estar operativo, temperatura >= 0°C`);
                try {
                    let nuevoEstado = "AVAILABLE"; 

                    if (estadosPrevios.hasOwnProperty(cp)) {
                        const oldState = estadosPrevios[cp];
                        
                        if (oldState === "SUPPLYING" || oldState === "AVAILABLE") {
                            nuevoEstado = "AVAILABLE";
                        } else if (oldState === "FUERA DE SERVICIO") {
                            nuevoEstado = "FUERA DE SERVICIO";
                        } else if (oldState === "DISCONNECTED") {
                            nuevoEstado = "DISCONNECTED";
                        }
                        
                        delete estadosPrevios[cp];
                    } else {
                        console.warn(`No se encontró estado previo para ${cp}, se pondrá en AVAILABLE por defecto.`);
                    }

                    await axios.put(
                        apiCentralUrl, 
                        { state: nuevoEstado }, 
                        { httpsAgent: httpsAgent }
                    );
                    console.log(`-> Estado de ${cp} restaurado a: ${nuevoEstado}`);

                } catch (err) {
                    console.error(`Error restaurando estado de ${cp}:`, err.message);
                }
            }
        }

        historialTemperaturas[cp] = tempActual;
        console.log(`[OK] CP: ${cp} | Ciudad: ${ciudad} | Temp: ${tempActual}°C`);

    } catch (error) {
        const msg = error.response ? error.response.data.message : error.message;
        console.error(`[ERROR] Ciudad: ${ciudad} | Causa: ${msg}`);
    }
}

try {
    const httpsOptions = { 
        key: fs.readFileSync("certServ.pem"), 
        cert: fs.readFileSync("certServ.pem"), 
    };

    https.createServer(httpsOptions, appSD)
        .listen(port, () => { 
            console.log("https API Server listening: " + port); 
        }); 
} catch (e) {
    console.error("No se pudieron cargar los certificados SSL (certServ.pem):", e.message);
}