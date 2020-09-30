"""
Sample API em Python para chamar e criar views no BigQuery
Pode ser acionada automaticamente com triggers com webhook

Para executar, necessita configurar variáveis de ambientes a seguir:

LOOKERSDK_BASE_URL=https://environment.cloud.looker.com:443
LOOKERSDK_CLIENT_ID=SEU_CLIENT_ID_LOOKER
LOOKERSDK_CLIENT_SECRET=SEU_CLIENT_SECRET_LOOKER

"""
import os
import requests
import json

from flask import Flask, render_template, request
from google.cloud import bigquery
import looker_sdk
from looker_sdk import error
from looker_sdk import models

# pylint: disable=C0103
app = Flask(__name__)
sdk = looker_sdk.init31()


def run_looker_inline(model, view, fields, filters):
    try:
        mod = models.WriteQuery(model=model, view=view,
                                fields=fields, filters=filters)
        sql = sdk.run_inline_query(result_format="sql", body=mod, cache=False)
        return sql
    except error.SDKError:
        print("error")
        # raise sdk_exceptions.RunInlineQueryError("Error running query")


def run_looker_sdk(reqs):
    sqls = []
    for req in reqs:
        print(req)
        input_model = json.loads(req)
        sql = run_looker_inline(model=input_model["model"], view=input_model["view"],
                                fields=input_model["fields"])
        sqls.append(sql)


def generate_biguery_view_from_explore(model_name, explore_name):
    # retrieve all fields that I want from the model and explore
    # API Call to pull in metadata about fields in a particular explore
    # sdk.lookml_model()

    exp = sdk.lookml_model_explore(
        lookml_model_name=model_name,
        explore_name=explore_name,
    )
    my_fields = []
    my_filters = []

    print(exp.conditionally_filter)
    print(exp.access_filters)
    print(exp.always_filter)

    # conditionally_filter
    if exp.conditionally_filter:
        for f in exp.conditionally_filter:
            my_filters.append(f)

    # access_filters
    if exp.access_filters:
        for f in exp.access_filters:
            my_filters.append(f)

    # always_filter
    if exp.always_filter:
        for f in exp.always_filter:
            my_filters.append(f)

    # Iterate through the field definitions and pull in the description, sql,
    # and other looker tags you might want to include in  your data dictionary.
    if exp.fields and exp.fields.dimensions:
        for dimension in exp.fields.dimensions:
            dim_def = {
                "field_type": "Dimension",
                "view_name": dimension.view_label,
                "field_name": dimension.name,
                "type": dimension.type,
                "description": dimension.description,
                "sql": dimension.sql,
            }
            my_fields.append(dim_def["field_name"].lstrip())

    return my_fields, my_filters


def create_view(sql, dataset, view_name):
    index = sql.rindex("GROUP")
    sql = sql[0:index]
    print("sql: " + sql)
    sql = "CREATE OR REPLACE VIEW " + dataset + "." + view_name + " AS " + sql
    client = bigquery.Client()
    query_job = client.query(sql)
    print("The query data:")
    for row in query_job:
        # Row values can be accessed by field name or index.
        print("Returned rows")

    return sql


@app.route('/', methods=['GET'])
def globo_create_view():
    model = request.args.get('model')
    view = request.args.get('view')

    if (model is None) or (view is None):
        print("Error have to receive model and view")
        return render_template('index.html', message="Error, missing model and view parameters")

    dataset = "looker"
    view_name = "view_" + model + "_" + view

    my_fields, my_filters = generate_biguery_view_from_explore(model, view)
    message = '\n'.join(my_fields)
    message = message + "Filters ------ \n"
    message = message + '\n'.join(my_fields)

    sql = run_looker_inline(model=model, view=view, fields=my_fields, filters=my_filters)
    sql = create_view(sql, dataset=dataset, view_name=view_name)

    return render_template('index.html',
                           message=sql,
                           Project="teste",
                           Service="servico",
                           Revision="revisao")


if __name__ == '__main__':
    server_port = os.environ.get('PORT', '8081')
    app.run(debug=False, port=server_port, host='0.0.0.0')
