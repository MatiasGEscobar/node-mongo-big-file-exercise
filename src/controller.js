const Records = require('./records.model');
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

const upload = async (req, res) => {
    const { file } = req;
    
    if (!file) {
        return res.status(400).json({ message: 'No file uploaded' });
    }

    const filePath = file.path;
    let processedRecords = 0;
    let totalRecords = 0;
    const batchSize = 15000; // Procesar en lotes de 15000 registros
    let batch = [];

    try {
        // Enviar respuesta inmediata y procesar en background
        res.status(202).json({
            message: 'File upload started. Processing in background...',
            status: 'processing'
        });

        console.log(`Starting to process file: ${file.originalname}`);
        const startTime = Date.now();

        // Crear stream de lectura del archivo CSV con buffer optimizado
        const stream = fs.createReadStream(filePath, { highWaterMark: 64 * 1024 }) // 64KB buffer
            .pipe(csv({
                // Mapear headers del CSV a nuestro modelo
                mapHeaders: ({ header }) => header.toLowerCase().trim(),
                skipEmptyLines: true,
                skipLinesWithError: true
            }));

        // Procesar cada línea del CSV (procesamiento ultra-rápido)
        for await (const row of stream) {
            // Procesamiento mínimo - solo asignar valores directos
            batch.push({
                id: +row.id || 0,  // Conversión más rápida
                firstname: row.firstname || '',
                lastname: row.lastname || '',
                email: row.email || '',
                email2: row.email2 || '',
                profession: row.profession || ''
            });
            
            totalRecords++;

            // Cuando el batch esté lleno, insertar en BD
            if (batch.length >= batchSize) {
                await insertBatch(batch);
                processedRecords += batch.length;
                batch = []; // Limpiar el batch
                
                // Log de progreso cada 100k registros (menos logs = más velocidad)
                if (processedRecords % 100000 === 0) {
                    const elapsed = (Date.now() - startTime) / 1000;
                    const rate = Math.round(processedRecords / elapsed);
                    console.log(`🚀 ${processedRecords} records (${rate}/sec)`);
                }
            }
        }

        // Procesar el último batch si tiene datos
        if (batch.length > 0) {
            await insertBatch(batch);
            processedRecords += batch.length;
        }

        const endTime = Date.now();
        const processingTime = (endTime - startTime) / 1000;

        console.log(`✅ Processing completed: ${processedRecords} records in ${processingTime}s`);
        console.log(`📊 Performance: ${Math.round(processedRecords / processingTime)} records/second`);

        // Limpiar archivo temporal
        fs.unlinkSync(filePath);

        // El usuario ya recibió respuesta 202, este log es para monitoreo

    } catch (error) {
        console.error('❌ Error processing file:', error.message);
        
        // Limpiar archivo temporal en caso de error
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
        }

        // NO enviar respuesta aquí porque ya se envió la 202
        // Solo logear el error para monitoreo
        console.error('💥 Processing failed:', error);
    }
};

// Función auxiliar para insertar lotes en MongoDB con máxima optimización
const insertBatch = async (batch) => {
    try {
        // Usar insertMany con opciones optimizadas para máxima performance
        await Records.insertMany(batch, {
            ordered: false,    // Continuar aunque algunos fallen
            lean: true,        // Optimización de performance  
            writeConcern: { w: 0 }, // Sin confirmación de escritura (máxima velocidad)
            bypassDocumentValidation: true // Saltar validaciones de schema
        });
    } catch (error) {
        // Manejar errores de duplicados u otros errores de BD
        if (error.code === 11000) {
            console.warn(`⚠️  Duplicate key error in batch insertion`);
        } else {
            console.error('❌ Error inserting batch:', error.message);
            throw error; // Re-lanzar errores críticos
        }
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