const express = require("express"); 
const appSD = express(); 

// Se define el puerto 
const port=3000; 

appSD.get("/",(req, res) => { 
    res.json({message:'Página de inicio de aplicación de ejemplo de SD'}) 
}); 

// Ejecutar la aplicacion 
appSD.listen(port, () => { 
    console.log(`Ejecutando la aplicación API REST de SD en el puerto ${port}`); 
});

const { MongoClient } = require('mongodb');
const client = new MongoClient("mongodb://127.0.0.1:27017");

let db;

async function connectDB() {
    await client.connect();
    db = client.db("evcharging_db"); // Conectamos a tu base de datos
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