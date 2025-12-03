const express = require("express"); 
const https = require('https'); 
const bodyParser = require("body-parser"); 
const fs = require("fs")
const appSD = express();

appSD.use(bodyParser.json()); 

// Se define el puerto 
const port=3000; 

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
    //response.send(cpId)
    const cp = await db.collection("charging_points").findOne({ id: cpId });
    if (cp) {
        response.status(200).json(cp);
    } else {
        response.status(404).json({ message: "Punto de carga no encontrado" });
    }
});

appSD.post("/addCP", async (request, response) => {
    const usuarioObj = {
        id: request.body.id,
        location: request.body.location,
        price: request.body.price
    }

    try {
        const cp = await db.collection("charging_points").insertOne(usuarioObj);
        response.status(200).json({ message: "Insertado correctamente" });
    } catch (error) {
        response.status(404).json(error.errmsg);
    }
});

appSD.delete("/deleteCP", (request, response) => {
        const usuarioObj = {
        id: request.body.id
    }
    try {
        // mirara aquí para cuando se intenta borrar un cp que no existe
        const cp = db.collection("charging_points").deleteOne(usuarioObj);
        response.status(200).json({ message: "Borrado correctamente" });
    } catch (error) {
        response.status(404).json(error.errmsg);
    }
});