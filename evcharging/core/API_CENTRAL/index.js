const express = require("express"); 
const https = require('https'); 
const bodyParser = require("body-parser"); 
const fs = require("fs")
const appSD = express();

const md5 = require("md5");
const prompt = require("prompt");
const uniqid = require("uniqid");

appSD.use(bodyParser.json()); 

// Se define el puerto 
const port = 5000; 

// Server http
https 
    .createServer( 
    { 
        key: fs.readFileSync("certServ.pem"), 
        cert: fs.readFileSync("certServ.pem"), 
    }, 
    appSD 
) 

.listen(port, () => { 
    console.log("https API Server listening: "+port); 
}); 

appSD.get("/",(req, res) => { 
    res.json({message:'Página de inicio de aplicación de ejemplo de SD HTTPS'}) 
}); 

// Endpoints base de datos

const { MongoClient } = require('mongodb');
const client = new MongoClient("mongodb://127.0.0.1:27017");

let db;

async function connectDB() {
    await client.connect();
    db = client.db("evcharging_db");
    console.log("Conectado a Mongo con Node.js");
}

connectDB();

appSD.get("/cps", async (request, response) => { 
    try {
        const cps = await db.collection("charging_points")
                            .find({})
                            .project({ _id: 0 }) 
                            .toArray();

        response.status(200).json(cps);

    } catch (error) {
        console.error("Error al obtener CPs:", error);
        response.status(500).send("Error interno del servidor");
    }
});

appSD.get("/cps/:cpId", async (request, response) => { 
    const {cpId} = request.params;
    const cp = await db.collection("charging_points").findOne({ id: cpId });
    if (cp) {
        response.status(200).json(cp);
    } else {
        response.status(404).json({ message: "Punto de carga no encontrado" });
    }
});

appSD.put("/changeState/:cpId", async (request, response) => { 
    const { cpId } = request.params; 
    const { state } = request.body;  

    if (!state) {
        return response.status(400).json({ message: "Falta el parámetro 'state' en el cuerpo de la petición" });
    }

    try {
        const result = await db.collection("charging_points").updateOne(
            { id: cpId },              
            { $set: { state: state } }
        );

        if (result.matchedCount === 0) {
            return response.status(404).json({ message: "Punto de carga no encontrado" });
        }

        response.status(200).json({ 
            message: "Estado actualizado correctamente", 
            cpId: cpId, 
            newState: state 
        });

    } catch (error) {
        console.error("Error al actualizar el estado del CP:", error);
        response.status(500).send("Error interno del servidor");
    }
});