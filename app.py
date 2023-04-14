import asyncio
from typing import Awaitable, Callable
import aiotrino
import pandas as pd
from aiohttp import web
import logging
import os
from io import BytesIO


logger = logging.getLogger("asyncio")
logger.setLevel(logging.DEBUG)

TRINO_HOST = os.getenv('TRINO_HOST','localhost')
TRINO_PORT = int(os.getenv('TRINO_PORT','8080'))
TOKEN_AUTH = os.getenv('TOKEN_AUTH')

async def handle_query(request):
    conn = aiotrino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="sync")
    cursor = await conn.cursor()
    body = await request.json()
    type = request.match_info.get('type')
    query = body['query']
    print(f"run: {type} for query : {query}")
    await cursor.execute(query)
    result = await cursor.fetchall()
    rows = remove_tz_from_rows(result)
    columns = [desc[0] for desc in cursor.description]
    print(f"return: {len(rows)} rows for query : {query}")
    df = pd.DataFrame(rows, columns=columns)
    await conn.close()
    return await format_type(df, type)

async def format_type(df: pd.DataFrame, type: str):
    
    if type == 'csv':
        csv = df.to_csv(index=False)
        response = web.Response(body=csv, content_type="text/csv")
        response.headers['Content-Disposition'] = "attachment; filename=resultado.csv"
        return response
    elif type == 'xslx':
        output_response = BytesIO()
        filename = 'resultado.xlsx'
        writer = pd.ExcelWriter(body=output_response, engine='xlsxwriter')
        df.to_excel(writer, sheet_name='dados')
        writer.save()
        output_response.seek(0)
        response = web.Response(body=output_response.read(), content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        response.headers['Content-Disposition'] = f"attachment; filename={filename}"
        return response
    elif type == 'json':
        json_data = df.to_json(orient='records')
        return web.Response(body=json_data, content_type='application/json')

    response = web.Response(text='Tipo não identificado')
    return response

@web.middleware
async def auth_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    try:
        token = request.headers.get('Authorization', None)
        if token != TOKEN_AUTH:
            raise web.HTTPForbidden(text="token nao autorizado")
        return await handler(request)
    except:
        raise

async def auth(request):
    print(f"classe:{request.headers['Host']}")

def remove_tz_from_rows(rows):
    rownum = []
    for row in rows[:3]:
        for column, value in enumerate(row):
            if len(str(row[column])) > 19 and (str(value).endswith('UTC') or str(row[column])[-6] in ['+','-']):
                if column not in rownum:
                    rownum.append(column)
    print(f"ajustando colunas {rownum}")
    for row in rows:
        for column in rownum:
            try: 
                if str(row[column])[-6] in ['+','-']:
                    row[column] = str(row[column])[:-6]
                elif str(value).endswith('UTC'):
                    row[column] = row[column].replace(' UTC','')
            except Exception as e:
                print(f"Erro ao verificar {column} em {row}")
                raise e

    return rows


    return df

async def init_app():
    app = web.Application(middlewares=[auth_middleware],logger=logger)
    app.add_routes([
        web.post('/query/{type}', handle_query)
    ])
    return app

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    web.run_app(app, port=8000)
