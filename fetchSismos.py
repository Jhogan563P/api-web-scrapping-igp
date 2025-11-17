import json
import boto3
import requests
import os

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ['TABLE_SISMOS']
table = dynamodb.Table(TABLE_NAME)

IGP_URL = "https://ultimosismo.igp.gob.pe/api/ultimo-sismo/ajaxb/2025"

def lambda_handler(event, context):
    try:
        # 1. Obtener JSON desde la API del IGP
        response = requests.get(IGP_URL, timeout=10)
        data = response.json()
        print("Respuesta del IGP (primeros 500 chars):", response.text[:500])

        # 2. Ordenar los registros por fecha_utc (los más nuevos primero)
        data_sorted = sorted(
            data,
            key=lambda x: x.get("fecha_utc", ""),
            reverse=True
        )

        # 3. Tomar los 10 más recientes
        sismos = data_sorted[:10]

        # 4. Guardar en DynamoDB
        for item in sismos:
            table.put_item(Item={
                "id": item.get("codigo", "sin-codigo"),
                "fecha_utc": item.get("fecha_utc", ""),
                "fecha_local": item.get("fecha_local", ""),
                "latitud": str(item.get("latitud", "")),
                "longitud": str(item.get("longitud", "")),
                "magnitud": str(item.get("magnitud", "")),
                "profundidad": str(item.get("profundidad", "")),
                "referencia": item.get("referencia", ""),
                "pdf_acelerometrico": item.get("reporte_acelerometrico_pdf", ""),
                "createdAt": item.get("createdAt", ""),
                "updatedAt": item.get("updatedAt", "")
            })

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Últimos 10 sismos almacenados correctamente",
                "count": len(sismos)
            })
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
