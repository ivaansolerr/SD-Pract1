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

let weatherStatus = {}; 

setInterval(() => {
    fs.readFile(FILE_API, 'utf8', (err, apiKeyData) => {
        if (err) {
            console.error(`Error: No se pudo leer ${FILE_API}.`);
            return;
        }
        const apiKey = apiKeyData.trim();
        if (apiKey) {
            leerCiudadesYConsultar(apiKey);
        } else {
            console.error("[ERROR] El archivo api.txt está vacío.");
        }
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

        if (tempActual < 0) {
            if (weatherStatus[cp] !== "FROZEN") {
                console.log(`[CLIMA] ❄️ Alerta: CP ${cp} en ${ciudad} tiene temperatura de congelación (${tempActual}°C).`);
                weatherStatus[cp] = "FROZEN";
            }
        } else {
            if (weatherStatus[cp] !== "OK") {
                console.log(`[CLIMA] ☀️ CP ${cp} en ${ciudad} recupera temperatura operativa (${tempActual}°C).`);
                weatherStatus[cp] = "OK";
            }
        }

    } catch (error) {
        // AQUÍ ESTÁ EL CAMBIO PRINCIPAL PARA DETECTAR ERRORES DE API
        if (error.response) {
            // El servidor respondió con un código de estado fuera del rango 2xx
            const status = error.response.status;
            
            if (status === 401) {
                console.error(`[ERROR CRÍTICO] ❌ La API Key es INCORRECTA o ha sido desactivada. Revise 'api.txt'.`);
            } else if (status === 404) {
                console.error(`[ERROR] ❌ Ciudad no encontrada: '${ciudad}' (CP: ${cp}).`);
            } else if (status === 429) {
                console.error(`[ERROR] ⚠️ Se ha excedido el límite de peticiones a la API.`);
            } else {
                console.error(`[ERROR] Respuesta de API: ${status} - ${error.response.data.message}`);
            }
        } else if (error.request) {
            // La petición se hizo pero no hubo respuesta (Error de red/internet)
            console.error(`[ERROR] 🌐 No hay conexión con OpenWeatherMap. Verifique su internet.`);
        } else {
            // Otro tipo de error
            console.error(`[ERROR] Error configurando la petición: ${error.message}`);
        }
    }
}

appSD.get("/weather-status", (req, res) => {
    res.json(weatherStatus);
});

try {
    const httpsOptions = {
        key: fs.readFileSync("certServ.pem"),
        cert: fs.readFileSync("certServ.pem"),
    };

    https.createServer(httpsOptions, appSD)
        .listen(port, () => {
            console.log("https EV_W Server listening: " + port);
        });
} catch (e) {
    console.error("Error SSL:", e.message);
}