import requests
import json
from datetime import datetime
import logging
import sys
# from psycopg2.extensions import AsIs, quote_ident
import psycopg2

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename=f"./tmp/log_{datetime.now()}",
                    filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def convert_to_date(dt_chk_str):
    res = dt_chk_str
    try:
        res = datetime.fromisoformat(dt_chk_str)
    except ValueError:
        pass
    finally:
        return res


def cr_frmt_str_f_list(lst):
    f_str = ""
    for i, v in enumerate(lst):
        if i == len(lst) - 1:
            f_str = f_str + "%s"
        else:
            f_str = f_str + "%s, "

    return f_str


def fetch_mapping(url):
    try:
        r = requests.get(url)
        logging.info(r.status_code)
        d = r.json()
        dt_fnd_d = dict(map(lambda key: (key, convert_to_date(d[key])), d.keys()))
        logging.info(dt_fnd_d)
        mapping = dict(map(lambda key: (key, type(dt_fnd_d[key]).__name__), d.keys()))
        return mapping
    except requests.ConnectionError as e:
        logging.error(e.errno)
        raise requests.ConnectionError


def gen_db_tbl(cols_map, db_map, tbl, cur):
    pg_col_map = dict(map(lambda key: (key, db_map[cols_map[key]]), cols_map.keys()))

    query = f"create table if not exists {tbl} ("
    for key in list(pg_col_map.keys()):
        if key == "id":
            query = query + f"""{key} {pg_col_map[key]} PRIMARY KEY, """
        elif key == list(cols_map.keys())[-1]:
            query = query + f"""{key} {pg_col_map[key]});"""
        else:
            query = query + f"""{key} {pg_col_map[key]}, """
    cur.execute(query)


def insert_data(data, tbl, cur):
    listed_data = []
    for d in data:
        listed_data.append(list(d.values()))
    fmt_str = cr_frmt_str_f_list(listed_data[0])

    for i, v in enumerate(listed_data):
        ins_q = f"insert into {tbl} values (" + fmt_str + ") ;"
        cur.execute(ins_q, v)


def fix_types_dict(chk_d):
    for k in chk_d.keys():
        if isinstance(k, str):
            chk_d[k] = convert_to_date(chk_d[k])
        # if isinstance(k, dou):
        #     chk_d[k] = float(chk_d[k])

    return chk_d


if __name__ == '__main__':
    conn = psycopg2.connect(host="127.0.0.1",
                            port=5432,
                            database="rst_db",
                            user="rst_usr",
                            password="qwerty123")
    conn.autocommit = True

    cur = conn.cursor()

    py_db_mapp = {
        "int": "int",
        "float": "double precision",
        "bool": "boolean",
        "str": "text",
        "bytes": "bytea",
        "datetime": "timestamp"
    }
    batch_size_arg = "?size=50"

    api_url = 'https://random-data-api.com/api/address/random_address'

    batched_api_url = api_url + batch_size_arg
    tbl_name = api_url.split("/")[4]

    r = requests.get(batched_api_url)
    logging.info(r.status_code)
    d = json.loads(r.text)

    dated_rows = []
    for row in d:
        dated_rows.append(fix_types_dict(row))
    logging.info("got dated_rows")
    logging.info(dated_rows)

    api_mapping = fetch_mapping(api_url)
    logging.info("got api mapping")
    logging.info(api_mapping)

    logging.info("running create table")
    gen_db_tbl(api_mapping, py_db_mapp, tbl_name, cur)
    logging.info("running insert")
    insert_data(dated_rows, tbl_name, cur)

    cur.close()
    conn.close()
