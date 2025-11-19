const chromium = require('@sparticuz/chromium');
const puppeteer = require('puppeteer-core');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, BatchWriteCommand } = require('@aws-sdk/lib-dynamodb');
const crypto = require('crypto');

// Configurar DynamoDB
const client = new DynamoDBClient({ region: 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(client);

const TABLE_NAME = 'Reportes_Sismos_IGP';
const URL = 'https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados';

/**
 * Genera un ID determinista basado en los datos del sismo
 */
function generateDeterministicId(sismo) {
  const raw = `${sismo.reporte_sismico}-${sismo.fecha_hora_local}-${sismo.magnitud}`;
  return crypto.createHash('sha256').update(raw).digest('hex');
}

/**
 * Lambda Handler para scrappear la tabla de sismos
 */
exports.lambda_handler = async () => {
  let browser = null;

  try {
    console.log('Iniciando scraping de sismos...');

    // Iniciar navegador optimizado para Lambda
    browser = await puppeteer.launch({
      args: chromium.args,
      defaultViewport: chromium.defaultViewport,
      executablePath: await chromium.executablePath(),
      headless: chromium.headless,
    });

    const page = await browser.newPage();
    page.setDefaultNavigationTimeout(60000);

    console.log(`Navegando a: ${URL}`);
    await page.goto(URL, { waitUntil: 'domcontentloaded' });

    console.log('Esperando la tabla...');
    await page.waitForSelector('table tbody tr', { timeout: 30000 });

    // Extraer sismos
    const sismos = await page.evaluate(() => {
      const rows = Array.from(document.querySelectorAll('table tbody tr'));

      return rows.map(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length < 5) return null;

        return {
          reporte_sismico: cells[0].textContent.trim(),
          referencia: cells[1].textContent.trim(),
          fecha_hora_local: cells[2].textContent.trim(),
          magnitud: cells[3].textContent.trim(),
          enlace_reporte: cells[4].querySelector('a')?.href || ''
        };
      }).filter(Boolean);
    });

    console.log(`Total sismos extraídos: ${sismos.length}`);

    if (!sismos.length) {
      return {
        statusCode: 200,
        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
        body: JSON.stringify({
          message: 'No se encontraron sismos en la página.',
          count: 0
        })
      };
    }

    // 🔥 SOLO TOMAR LOS 10 ÚLTIMOS
    const ultimos10 = sismos.slice(0, 10);
    console.log(`Guardando solo los últimos 10 sismos`);

    await saveToDynamoDB(ultimos10);

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({
        message: 'Scraping completado exitosamente',
        count: ultimos10.length,
        timestamp: new Date().toISOString(),
        sismos: ultimos10
      })
    };

  } catch (error) {
    console.error('ERROR en scraping:', error);

    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
      body: JSON.stringify({
        message: 'Error durante el scraping',
        error: error.message
      })
    };

  } finally {
    if (browser) {
      await browser.close();
      console.log('Browser cerrado');
    }
  }
};

/**
 * Guardar sismos en DynamoDB
 */
async function saveToDynamoDB(sismos) {
  const timestamp = new Date().toISOString();

  const batch = sismos.map(sismo => ({
    PutRequest: {
      Item: {
        id: generateDeterministicId(sismo), // 🔥 ID SIN DUPLICADOS
        reporte_sismico: sismo.reporte_sismico,
        referencia: sismo.referencia,
        fecha_hora_local: sismo.fecha_hora_local,
        magnitud: parseFloat(sismo.magnitud) || sismo.magnitud,
        enlace_reporte: sismo.enlace_reporte,
        scraped_at: timestamp
      }
    }
  }));

  const command = new BatchWriteCommand({
    RequestItems: {
      [TABLE_NAME]: batch
    }
  });

  try {
    await docClient.send(command);
    console.log(`Los 10 últimos sismos fueron guardados en DynamoDB`);
  } catch (error) {
    console.error(`Error guardando en DynamoDB:`, error);
    throw error;
  }
}
