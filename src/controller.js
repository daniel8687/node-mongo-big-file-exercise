const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const Records = require('./records.model');

const upload = async (req, res) => {
    const {file} = req;

    try {
        if (!file) {
            return res.status(400).json({ error: 'No se subió ningún archivo' });
        }

        const filePath = `./_temp/${file.filename}`;
        const results = [];
        const BATCH_SIZE = 1000;
        let batch = [];

        const stream = fs.createReadStream(filePath)
            .pipe(csv({ separator: ',' }));

        stream.on('data', async (data) => {
            batch.push(data);
            if (batch.length >= BATCH_SIZE) {
            stream.pause();
            await Records.insertMany(batch);
            batch = [];
            stream.resume();
            }
        });

        stream.on('end', async () => {
            if (batch.length > 0) {
            await Records.insertMany(batch);
            }

            fs.unlink(filePath, (err) => {
            if (err) console.error('Error al eliminar archivo:', err);
            });

            res.status(200).json({ message: 'Archivo procesado con éxito' });
        });

        stream.on('error', (err) => {
            console.error('Error al procesar archivo:', err);
            res.status(500).json({ error: 'Error procesando archivo' });
        });
    } catch (err) {
        console.error('Error general:', err);
        res.status(500).json({ error: 'Error interno del servidor' });
    }
};

const list = async (_, res) => {
    try {
        const data = await Records
            .find({})
            .limit(10)
            .lean();
        
        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};
