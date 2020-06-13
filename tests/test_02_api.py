# test_hello_add.py
import app
from flask import Flask, json


def test_kpi1_all():
    endpoint = 'http://127.0.0.1:5000/kpi1/?interval=5-minute&from=2017-03-01_10:00:00&to=2017-03-01_10:05:00'
    expected_data = [
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "service_id": 1,
            "total_bytes": 16100,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "service_id": 3,
            "total_bytes": 11500,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "service_id": 2,
            "total_bytes": 9260,
            "interval": "5-minute"
        }
    ]

    with app.app.test_client() as client:
        response = client.get(endpoint)
        actual_data = json.loads(response.get_data(as_text=True))
        assert response.status_code == 200
        assert actual_data == expected_data


def test_kpi2_all():
    endpoint = 'http://127.0.0.1:5000/kpi2/?interval=5-minute&from=2017-03-01_10:00:00&to=2017-03-01_10:05:00'
    expected_data = [
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "cell_id": 1001,
            "number_of_unique_users": 4,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "cell_id": 5005,
            "number_of_unique_users": 3,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "cell_id": 1000,
            "number_of_unique_users": 3,
            "interval": "5-minute"
        }
    ]

    with app.app.test_client() as client:
        response = client.get(endpoint)
        actual_data = json.loads(response.get_data(as_text=True))
        assert response.status_code == 200
        assert actual_data == expected_data


def test_kpi1_one():
    endpoint = 'http://127.0.0.1:5000/kpi1/?interval=1-hour'
    expected_data = [
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "service_id": 1,
            "total_bytes": 32200,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "service_id": 3,
            "total_bytes": 23000,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "service_id": 2,
            "total_bytes": 18520,
            "interval": "1-hour"
        }
    ]

    with app.app.test_client() as client:
        response = client.get(endpoint)
        actual_data = json.loads(response.get_data(as_text=True))
        assert response.status_code == 200
        assert actual_data == expected_data


def test_kpi2_one():
    endpoint = 'http://127.0.0.1:5000/kpi2/?interval=1-hour'
    expected_data = [
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "cell_id": 1001,
            "number_of_unique_users": 4,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "cell_id": 5005,
            "number_of_unique_users": 3,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "cell_id": 1000,
            "number_of_unique_users": 3,
            "interval": "1-hour"
        }

    ]

    with app.app.test_client() as client:
        response = client.get(endpoint)
        actual_data = json.loads(response.get_data(as_text=True))
        assert response.status_code == 200
        assert actual_data == expected_data


def test_kpi1_none():
    endpoint = 'http://127.0.0.1:5000/kpi1/'
    expected_data = [
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "service_id": 1,
            "total_bytes": 32200,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "service_id": 3,
            "total_bytes": 23000,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "service_id": 2,
            "total_bytes": 18520,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "service_id": 1,
            "total_bytes": 16100,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "service_id": 3,
            "total_bytes": 11500,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "service_id": 2,
            "total_bytes": 9260,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:05:00",
            "interval_end_timestamp": "2017-03-01 10:10:00",
            "service_id": 1,
            "total_bytes": 16100,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:05:00",
            "interval_end_timestamp": "2017-03-01 10:10:00",
            "service_id": 3,
            "total_bytes": 11500,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:05:00",
            "interval_end_timestamp": "2017-03-01 10:10:00",
            "service_id": 2,
            "total_bytes": 9260,
            "interval": "5-minute"
        }
    ]

    with app.app.test_client() as client:
        response = client.get(endpoint)
        data = response.get_data(as_text=True)
        print("data", data)
        actual_data = json.loads(response.get_data(as_text=True))
        assert response.status_code == 200
        assert actual_data == expected_data


def test_kpi2_none():
    endpoint = 'http://127.0.0.1:5000/kpi2/'
    expected_data = [
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "cell_id": 1001,
            "number_of_unique_users": 4,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "cell_id": 5005,
            "number_of_unique_users": 3,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 11:00:00",
            "cell_id": 1000,
            "number_of_unique_users": 3,
            "interval": "1-hour"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "cell_id": 1001,
            "number_of_unique_users": 4,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "cell_id": 5005,
            "number_of_unique_users": 3,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:00:00",
            "interval_end_timestamp": "2017-03-01 10:05:00",
            "cell_id": 1000,
            "number_of_unique_users": 3,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:05:00",
            "interval_end_timestamp": "2017-03-01 10:10:00",
            "cell_id": 1001,
            "number_of_unique_users": 4,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:05:00",
            "interval_end_timestamp": "2017-03-01 10:10:00",
            "cell_id": 5005,
            "number_of_unique_users": 3,
            "interval": "5-minute"
        },
        {
            "interval_start_timestamp": "2017-03-01 10:05:00",
            "interval_end_timestamp": "2017-03-01 10:10:00",
            "cell_id": 1000,
            "number_of_unique_users": 3,
            "interval": "5-minute"
        }
    ]

    with app.app.test_client() as client:
        response = client.get(endpoint)
        actual_data = json.loads(response.get_data(as_text=True))
        assert response.status_code == 200
        assert actual_data == expected_data
