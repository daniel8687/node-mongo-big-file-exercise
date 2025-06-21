const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const Records = require('./records.model');

const upload = async (req, res) => {
    const { file } = req;

    try {
        if (!file) {
            return res.status(400).json({ error: 'No se subió ningún archivo' });
        }

        const filePath = `./_temp/${file.filename}`;
        const BATCH_SIZE = 5000;
        let batch = [];
        let pendingWrites = 0;
        let maxPendingWrites = 5;

        const stream = fs.createReadStream(filePath)
            .pipe(csv({ separator: ',', strict: true }));

        const processBatch = async (currentBatch) => {
            pendingWrites++;
            try {
                await Records.insertMany(currentBatch, { ordered: false, lean: true });
            } catch (err) {
                console.error('Error al insertar el batch:', err);
            }
            pendingWrites--;
        };

        stream.on('data', (data) => {
            batch.push(data);
            if (batch.length >= BATCH_SIZE && pendingWrites < maxPendingWrites) {
                const currentBatch = batch;
                batch = [];
                processBatch(currentBatch);
            }
        });

        stream.on('end', async () => {
            const waitForPending = async () => {
                if (pendingWrites > 0) {
                    await new Promise(resolve => setTimeout(resolve, 100));
                    return waitForPending();
                }
                return true;
            };

            await waitForPending();

            if (batch.length > 0) {
                await processBatch(batch);
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
