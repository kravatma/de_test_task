import json
import pandas
import datetime


def read_json(path, mode='r'):
    with open(path, mode) as f:
        return json.load(f)


def validate_dict_values(d, model):
    """

    :param d: словарь подлежащий проверке {"key": value}
    :param model: словарь вида {"key": value_type}
    :return:
    """
    for k, v in d.items():
        if type(v) != model[k]:
            raise Exception(f"Ожидаемый тип для {k} ({v}) - {model[k]}")


def validate_string_is_date(s):
    try:
        datetime.date.fromisoformat(s)
    except ValueError:
        raise ValueError(f"{s} некорректный формат даты")


def clients_data_check(data):
    client_ids = [d['client_id'] for d in data]
    if len(client_ids) > len(set(client_ids)):
        raise Exception('client_id не уникальны')
    m = {"client_id": int, "name": str, "signup_date": str, "country": str}
    for d in data:
        validate_dict_values(d, model=m)
        validate_string_is_date(d['signup_date'])


def transactions_data_check(data):
    transaction_ids = [d['transaction_id'] for d in data]
    if len(transaction_ids) > len(set(transaction_ids)):
        raise Exception('transaction_id не уникальны')
    m1 = {"transaction_id": int, "client_id": int, "transaction_date": str,
          "amount": float, "transaction_type": str, "product_details": list}
    m2 = {"product_id": str, "quantity": int, "price_per_unit": float}
    for d in data:
        validate_dict_values(d, model=m1)
        validate_string_is_date(d['transaction_date'])
        for p in d['product_details']:
            validate_dict_values(p, model=m2)


def calc_total_cost(product_details):
    return sum(d['quantity'] * d['price_per_unit'] for d in product_details)


if __name__ == "__main__":
    clients = read_json('clients.json')
    clients_data_check(clients)
    transactions = read_json('transactions.json')
    transactions_data_check(transactions)
    for t in transactions:
        t['products_count'] = len(t['product_details'])
        t['total_cost'] = calc_total_cost(t['product_details'])

    cdf = pandas.DataFrame(clients)
    tdf = pandas.DataFrame(transactions)

    tdf.to_csv("task3_transactions_enriched.csv", sep=';', header=True)

    tdf_grb = tdf.groupby('client_id').agg({"amount": "sum", "total_cost": "mean", "products_count": "sum"})
    cdf_grb = cdf.groupby('country', as_index=False)[['client_id']].count().sort_values('client_id', ascending=False).head(5)
    cdf_grb.rename(columns={'client_id': 'top5_countries'}, inplace=True)
    cdf_grb['top5_countries'] = 1
    cdf = cdf.merge(cdf_grb, on='country', how='left').set_index('client_id')
    clients_pv_final = tdf_grb.join(cdf[['top5_countries']])
    clients_pv_final.fillna({'top5_countries': 0}, inplace=True)
    clients_pv_final.to_csv('task3_clients_pivot.csv', index=True, sep=';')
