import json
import boto3
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("SismosIGP")

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimo-sismo/sismos-reportados"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Abrir la página
            page.goto(url)

            # Esperar a que Angular cargue la tabla
            page.wait_for_selector("table", timeout=10000)

            # Obtener el HTML completo ya renderizado
            html = page.content()
            browser.close()

        # Procesar HTML con BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table tbody tr")

        sismos = []

        for row in rows[:10]:   # Los primeros 10
            cols = [col.text.strip() for col in row.find_all("td")]

            sismos.append({
                "fecha": cols[0],
                "hora": cols[1],
                "magnitud": cols[2],
                "profundidad": cols[3],
                "latitud": cols[4],
                "longitud": cols[5],
                "referencia": cols[6]
            })

        # Guardar en DynamoDB
        for item in sismos:
            table.put_item(Item=item)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Scraping realizado correctamente", "total": len(sismos)})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
