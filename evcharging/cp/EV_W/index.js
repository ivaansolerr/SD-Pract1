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

// --- NUEVO: Objeto para controlar el estado de los errores ---
let systemStatus = {
    apiFileError: false,      // Error leyendo api.txt
    citiesFileError: false,   // Error leyendo ciudades.txt
    networkError: false,      // Error de conexión o servidor caído
    authError: false          // Error de API Key (401)
};
// -------------------------------------------------------------

setInterval(() => {
    fs.readFile(FILE_API, 'utf8', (err, apiKeyData) => {
        if (err) {
            console.error(`Error: No se pudo leer ${FILE_API}.`);
            systemStatus.apiFileError = true; // Marcamos error
            return;
        }

        // RECOVERY CHECK: Si antes fallaba el archivo y ahora funciona
        if (systemStatus.apiFileError) {
            console.log(`[SISTEMA] ✅ El archivo ${FILE_API} ha sido leído correctamente de nuevo.`);
            systemStatus.apiFileError = false;
        }

        const apiKey = apiKeyData.trim();
        if (apiKey) {
            leerCiudadesYConsultar(apiKey);
        } else {
            console.error("[ERROR] El archivo api.txt está vacío.");
            systemStatus.apiFileError = true;
        }
    });
}, 4000);

function leerCiudadesYConsultar(apiKey) {
    fs.readFile(FILE_CIUDADES, 'utf8', (err, data) => {
        if (err) {
            console.error(`Error leyendo ${FILE_CIUDADES}:`, err.message);
            systemStatus.citiesFileError = true; // Marcamos error
            return;
        }

        // RECOVERY CHECK: Si antes fallaba ciudades.txt y ahora funciona
        if (systemStatus.citiesFileError) {
            console.log(`[SISTEMA] ✅ El archivo ${FILE_CIUDADES} ha sido leído correctamente de nuevo.`);
            systemStatus.citiesFileError = false;
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
        
        // --- RECOVERY CHECKS DE RED Y API ---
        // 1. Si teníamos error de conexión y ahora funciona:
        if (systemStatus.networkError) {
            console.log(`[CONEXIÓN] 🟢 Conexión con OpenWeatherMap restablecida.`);
            systemStatus.networkError = false;
        }
        // 2. Si teníamos error de clave incorrecta y ahora funciona:
        if (systemStatus.authError) {
            console.log(`[API] 🟢 La API Key vuelve a ser válida y funcional.`);
            systemStatus.authError = false;
        }
        // ------------------------------------

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
        if (error.response) {
            const status = error.response.status;
            
            if (status === 401) {
                // Solo mostramos el error si no lo hemos mostrado ya (para evitar spam)
                if (!systemStatus.authError) {
                    console.error(`[ERROR CRÍTICO] ❌ La API Key es INCORRECTA o ha sido desactivada. Revise 'api.txt'.`);
                    systemStatus.authError = true; 
                }
            } else if (status === 404) {
                console.error(`[ERROR] ❌ Ciudad no encontrada: '${ciudad}' (CP: ${cp}).`);
            } else if (status === 429) {
                console.error(`[ERROR] ⚠️ Se ha excedido el límite de peticiones a la API.`);
                systemStatus.networkError = true; // Tratamos el rate limit como error de red/servicio
            } else {
                console.error(`[ERROR] Respuesta de API: ${status} - ${error.response.data.message}`);
            }
        } else if (error.request) {
            // Error de red (sin internet o servidor caído)
            if (!systemStatus.networkError) {
                console.error(`[ERROR] 🌐 No hay conexión con OpenWeatherMap. Verifique su internet.`);
                systemStatus.networkError = true;
            }
        } else {
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