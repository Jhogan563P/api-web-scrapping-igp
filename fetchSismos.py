import json
import boto3
import requests
import os

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ['TABLE_SISMOS']
table = dynamodb.Table(TABLE_NAME)

# API REAL que usa la página del IGP
IGP_URL = "https://ultimosismo.igp.gob.pe/ultimo-sismo/webservices/lista_sismos"

def lambda_handler(event, context):
    try:
        # 1. Obtener JSON desde la API del IGP
        response = requests.get(IGP_URL, timeout=10)
        data = response.json()

        # 2. Tomar los 10 primeros sismos
        sismos = data[:10]

        # 3. Guardar en DynamoDB
        for item in sismos:
            table.put_item(Item={
                "id": str(item["id"]),
                "fecha": item.get("fecha", "-"),
                "latitud": str(item.get("latitud", "")),
                "longitud": str(item.get("longitud", "")),
                "magnitud": str(item.get("magnitud", "")),
                "profundidad": str(item.get("profundidad", "")),
                "lugar": item.get("lugar", "-"),
            })

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Últimos 10 sismos almacenados correctamente en DynamoDB",
                "count": len(sismos)
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
