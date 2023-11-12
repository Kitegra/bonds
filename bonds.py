
import requests
import pandas as pd
from urllib import parse
import datetime



def flatten( j:dict, blockname:str):
    """
    Собираю двумерный массив (словарь)
    :param j:
    :param blockname:
    :return:
    """
    return [{str.lower(k) : r[i] for i, k in enumerate(j[blockname]['columns'])} for r in j[blockname]['data']]


#формерование запроса 
def query( method : str, **kwargs):
    """
    Отправляю запрос к ISS MOEX
    :param method:
    :param kwargs:
    :return:
    """
    try:
        url = "https://iss.moex.com/iss/%s.json" % method
        if kwargs:
            if '_from' in kwargs: kwargs['from'] = kwargs.pop('_from') # костыль - from нельзя указывать как аргумент фн, но в iss оно часто исп
            url += "?" + parse.urlencode(kwargs)

        # не обязательно
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',}

        r = requests.get(url, headers=headers)
        r.encoding = 'utf-8'
        j = r.json()
        return j

    except Exception as e:
        print("query error %s" % str(e))
        return None


def rows_to_dict(j:dict, blockname:str, field_key='name', field_value='value'):
        """
        Для преобразования запросов типа /securities/:secid.json (спецификация бумаги)
        в словарь значений
        :param j:
        :param blockname:
        :param field_key:
        :param field_value:
        :return:
        """
        return {str.lower(r[field_key]) : r[field_value] for r in flatten(j, blockname)}

def get_yield(secid: str):
    path = f"history/engines/stock/markets/bonds/sessions/3/securities/{secid}"
    _from = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    _r = flatten(query(path, _from=_from), 'history')
# если сделок не было, то что-то нужно записать в базу чтобы не запрашивать облигу сегодня ещё
    # todo: проверить - гипотетически пустые ответы могут быть сбоем
    if len(_r) < 1: return {'price' : 0, 'yieldsec' : 0, 'tradedate' : datetime.datetime.now().strftime("%Y-%m-%d"), 'volume' : 0}

    return {
        'price' : _r[-1]['close'],
        'yieldsec' : _r[-1]['yieldclose'],
        'tradedate' : _r[-1]['tradedate'],
        'volume' : _r[-1]['volume'],
    }

#запрос всех облигаций базовый 
def get_bonds():
    f = []
    for p in range(1, 2):

        s = f"[{'■' * (p // 10)} {'○' * (10 - (p // 10))}] {p}"
        print(s, end="\r")

        j = query("securities", group_by="group", group_by_filter="stock_bonds", limit=100, start=(p-1)*100)
        f.append(flatten(j, 'securities')) 
        if len(f) < 1:
            break

    return(f)







df = pd.DataFrame(columns=['Id', 'price', 'yieldsec', 'volume', 'secid', 'name', 'is_traded', 'emitent_id', 'shortname', 'matdate', 'initialfacevalue', 'issuesize', 'isqualifiedinvestors', 'couponpercent', 'typename'])

bonds_all = get_bonds()

k=0
for i in range(len(bonds_all)):

    s = f"[{'■' * (i // 10)} {'○' * (10 - (i // 10))}] {i}"
    print(s, end="\r")

    for j in range(len(bonds_all[i])):

        s = f"[{'■' * (j // 10)} {'○' * (10 - (j // 10))}] {j}"
        print(s, end="\r")

        if bonds_all[i][j]["is_traded"] == 1:
            secid = bonds_all[i][j]["secid"]
            yiald_bond = get_yield(secid)
            if yiald_bond["price"] != None:

                rows_to_dict_bonds = rows_to_dict(query(f"securities/{secid}"), 'description')
                
                if not("matdate" in rows_to_dict_bonds):
                    continue  
                if not("issuesize" in rows_to_dict_bonds):
                    continue 
                if not("isqualifiedinvestors" in rows_to_dict_bonds):
                    continue 
                if not("couponpercent" in rows_to_dict_bonds):
                    continue
                if not("typename" in rows_to_dict_bonds):
                    continue  

                df.loc[k] = [
                    k + 1,
                    yiald_bond["price"],  # Случайная цена
                    yiald_bond["yieldsec"],  # Случайная доходность
                    yiald_bond["volume"],  # Случайный объем
                    bonds_all[i][j]["secid"],  # Случайный secid
                    bonds_all[i][j]["name"],  # Случайное имя
                    bonds_all[i][j]["is_traded"],  # Случайный флаг is_traded
                    bonds_all[i][j]["emitent_id"],  # Случайный emitent_id
                    bonds_all[i][j]["shortname"],  # Случайное shortname
                    rows_to_dict_bonds["matdate"],  # Случайная дата
                    rows_to_dict_bonds["initialfacevalue"],  # Случайное initialfacevalue
                    rows_to_dict_bonds["issuesize"],  # Случайный issuesize
                    rows_to_dict_bonds["isqualifiedinvestors"],  # Случайный флаг isqualifiedinvestors
                    rows_to_dict_bonds["couponpercent"],  # Случайный couponpercent
                    rows_to_dict_bonds["typename"]  # Случайный typename
                ]

                k+=1



# Выводим DataFrame
print(df)

df.to_csv('bonds.csv', index=False)
