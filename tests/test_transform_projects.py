import pandas as pd
from plugins.steps.wb_data import transform


class MockTI:
    def __init__(self, pulled):
        self.pulled = pulled
        self.pushed = {}

    def xcom_pull(self, task_ids, key):
        return self.pulled

    def xcom_push(self, key, value):
        self.pushed[key] = value


def test_transform_projects():
    df_project = pd.DataFrame([
        {
            "id": "1",
            "countryname": "CountryA",
            "boardapprovaldate": "2000-01-01",
            "closingdate": "2001-01-01",
            "sector1": "SectorA",
            "totalamt": 100,
        },
        {
            "id": "2",
            "countryname": "CountryB",
            "boardapprovaldate": "2000-01-01",
            "closingdate": None,
            "sector1": "SectorB",
            "totalamt": 200,
        },
        {
            "id": "3",
            "countryname": "NonCountry",
            "boardapprovaldate": "2000-01-01",
            "closingdate": "2001-01-01",
            "sector1": "SectorC",
            "totalamt": 300,
        },
    ])

    df_vvp = pd.DataFrame(
        {
            "Country Name": ["CountryA", "CountryB"],
            "Country Code": ["CAA", "CBB"],
            "Indicator Name": ["GDP", "GDP"],
            "Indicator Code": ["GDP", "GDP"],
            "2000": [1.0, 2.0],
        }
    )

    df_population = pd.DataFrame(
        {
            "Country Name": ["CountryA", "CountryB"],
            "Country Code": ["CAA", "CBB"],
            "Indicator Name": ["Population", "Population"],
            "Indicator Code": ["POP", "POP"],
            "2000": [10.0, 20.0],
        }
    )

    country_mapping = {
        "CountryA": "CAA",
        "CountryB": "CBB",
        "NonCountry": "NON",
    }
    non_countries = ["NonCountry"]

    extracted = {
        "df_project": df_project,
        "df_vvp": df_vvp,
        "df_population": df_population,
        "country_mapping": country_mapping,
        "non_countries": non_countries,
    }

    ti = MockTI(extracted)
    transform(ti=ti)
    result = ti.pushed["transformed_data"]

    assert set(result["countrycode"]) == {"CAA", "CBB"}
    assert "NonCountry" not in result["countryname_off"].tolist()
    assert all(result["target"] == (result["year_close"] != "").astype(int))
