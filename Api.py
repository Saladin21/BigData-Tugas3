from flask import Flask
from flask import jsonify
from flask import request
from datetime import time
from sqlite3 import *
import psycopg2

db_config = psycopg2.connect(
    database="pyspark",
    host="localhost",
    user="bigdata",
    password=""
)

app = Flask(__name__)
app.secret_key = "DEVELOPMENT_SECRET_KEY"
app.config['SESSION_PERMANENT'] = False

@app.route("/getData")
def getData() -> None:
    try:
        start = request.args.get('start')
        end = request.args.get('end')
        social_media = request.args.get('social_media')

        cur = db_config.cursor()
        print(social_media)
        query = f"SELECT sum(count), sum(unique_count) FROM social_media_dataframe where time >= '{start}' AND time <= '{end}'  AND social_media = '{social_media}'"

        ret = []
        cur.execute(query)
        records = cur.fetchall()
        print(records[0])
        for record in records:
            ret.append({
                'Count': record[0],
                'Unique Count': record[1],
            })

        return jsonify({'start':start,'end':end,'social_media':social_media,'record': ret})
    except psycopg2.Error as e:
        print("Error")
        print(e)
        db_config.rollback()
    finally:
        cur.close()

if __name__ == '__main__':
    app.run(debug=True,port=3000,host='0.0.0.0')