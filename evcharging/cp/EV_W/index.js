const express = require("express"); 
const https = require('https'); 
const bodyParser = require("body-parser"); 
const fs = require("fs");
const axios = require("axios"); 

const appSD = express();
appSD.use(bodyParser.json()); 

const port = 4000; 
const FILE_CIUDADES = "ciudades.txt";
const FILE_API = "api.txt";

let historialTemperaturas = {}; 

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

        // si la temperatura pasa de 0 a 1        
        if (historialTemperaturas.hasOwnProperty(cp)) {
            const tempAnterior = historialTemperaturas[cp];
            // aquí habrá que consumir la api de central para que esta cambie el estado de los cps
            // a no disponible
            if (tempAnterior >= 0 && tempActual < 0) {
                console.log(`El CP ${cp} deja de estar disponbile, su temperatura es menor a 0 grados`);
            }
            else if (tempAnterior < 0 && tempActual >= 0) {
                console.log(`El CP ${cp} vuelve a estar disponible, la temperatura es superior a 0 grados`);
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