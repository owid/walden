"""Fetch data on fossil fuels energy production from The Shift Data Portal.

"""

import json
import re
import tempfile
from pathlib import Path
from time import sleep
from typing import List

import click
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
from owid.datautils import dataframes
from tqdm.auto import tqdm

from owid.walden import Dataset
from owid.walden import add_to_catalog, files

# Time (in seconds) to wait between consecutive queries.
TIME_BETWEEN_QUERIES = 1
# Maximum number of countries to fetch in each query.
MAX_NUM_COUNTRIES_PER_QUERY = 10
# Namespace.
NAMESPACE = "shift"
# Path to metadata file.
METADATA_FILE = Path(__file__).parent / "shift.meta.yml"

# Parameters for query.
SHIFT_URL = "https://www.theshiftdataportal.org/"
ENERGY_UNIT = "TWh"
# First year with data (make it any older year than 1900, in case they have data before this year).
START_YEAR = 1900
# Last year with data (make it an arbitrary future year, in case they have recent data).
END_YEAR = 2100
# List of energy sources.
ENERGY_SOURCES = ["coal", "gas", "oil"]
# List of countries and regions.
SHIFT_COUNTRIES = [
    "Africa",
    "Albania",
    "Algeria",
    "Angola",
    "Argentina",
    "Armenia",
    "Asia%20and%20Oceania",
    "Australia",
    "Austria",
    "Azerbaijan",
    "Bahrain",
    "Bangladesh",
    "Belarus",
    "Belgium",
    "Benin",
    "Bolivia",
    "Botswana",
    "Brazil",
    "Brunei%20Darussalam",
    "Bulgaria",
    "Burma",
    "Cambodia",
    "Cameroon",
    "Canada",
    "Central%20and%20South%20America",
    "Chile",
    "China",
    "Colombia",
    "Congo",
    "Croatia",
    "Cuba",
    "Cyprus",
    "Denmark",
    "Dominican%20Republic",
    "EU28",
    "Ecuador",
    "Egypt",
    "El%20Salvador",
    "Eritrea",
    "Estonia",
    "Ethiopia",
    "Eurasia",
    "Europe",
    "Finland",
    "France",
    "Gabon",
    "Georgia",
    "Germany",
    "Ghana",
    "Gibraltar",
    "Greece",
    "Guatemala",
    "Haiti",
    "Honduras",
    "Hungary",
    "Iceland",
    "India",
    "Indonesia",
    "Iraq",
    "Ireland",
    "Israel",
    "Italy",
    "Ivory%20Coast",
    "Jamaica",
    "Japan",
    "Jordan",
    "Kazakhstan",
    "Kenya",
    "Kosovo",
    "Kuwait",
    "Kyrgyzstan",
    "Latvia",
    "Lebanon",
    "Libya",
    "Lithuania",
    "Malaysia",
    "Malta",
    "Mauritius",
    "Mexico",
    "Middle%20East",
    "Mongolia",
    "Montenegro",
    "Morocco",
    "Mozambique",
    "NZ",
    "Namibia",
    "Nepal",
    "Netherlands",
    "Nicaragua",
    "Niger",
    "Nigeria",
    "North%20America",
    "Norway",
    "OECD",
    "OPEC",
    "Oman",
    "Pakistan",
    "Panama",
    "Paraguay",
    "Persian%20Gulf",
    "Peru",
    "Poland",
    "Portugal",
    "Qatar",
    "Romania",
    "Russian%20Federation%20%26%20USSR",
    "Senegal",
    "Serbia",
    "Singapore",
    "Slovakia",
    "Slovenia",
    "South%20Africa",
    "Spain",
    "Sudan",
    "Suriname",
    "Sweden",
    "Tajikistan",
    "Thailand",
    "Togo",
    "Tunisia",
    "Turkey",
    "Ukraine",
    "United%20Kingdom",
    "United%20States%20of%20America",
    "Uruguay",
    "Uzbekistan",
    "World",
    "Yemen",
    "Zambia",
    "Zimbabwe",
]


def prepare_query_url(energy_source: str, countries: List[str]) -> str:
    """Prepare a query URL to request data for a specific energy source and a list of countries.

    Parameters
    ----------
    energy_source : str
        Name of energy source (e.g. "coal").
    countries : list
        Countries to include in the query.

    Returns
    -------
    query_url : str
        Query URL to use to request data.

    """
    # Prepare a query url for request.
    query_url = (
        f"{SHIFT_URL}energy/{energy_source}?chart-type=line&chart-types=line&chart-types=ranking&"
        f"disable-en=false&energy-unit={ENERGY_UNIT}"
    )
    # Add each country to the url.
    for country in countries:
        query_url += f"&group-names={country}"
    # Add some conditions to the query (not all of them may be necessary).
    query_url += (
        f"&is-range=true&dimension=total&end={END_YEAR}&start={START_YEAR}&multi=true&type=Production&"
        f"import-types=Imports&import-types=Exports&import-types=Net%20Imports"
    )

    return query_url


def fetch_data_for_energy_source_and_a_list_of_countries(
    energy_source: str, countries: List[str]
) -> pd.DataFrame:
    """Fetch data from Shift for a specific energy source and a list of countries.

    Parameters
    ----------
    energy_source : str
        Name of energy source (e.g. "coal").
    countries : list
        Countries to include in the query.

    Returns
    -------
    df : pd.DataFrame
        Shift data.

    """
    query_url = prepare_query_url(energy_source=energy_source, countries=countries)
    soup = BeautifulSoup(requests.get(query_url).content, "html.parser")
    data = json.loads(
        soup.find(
            "script",
            {"type": "application/json", "id": re.compile(r"^((?!tb-djs).)*$")},
        ).string
    )

    fields = data["props"]["apolloState"]
    elements = {}
    years = []
    for key in list(fields):
        if ("name" in fields[key]) and ("data" in fields[key]):
            if fields[key]["name"] in countries:
                elements[fields[key]["name"]] = fields[key]["data"]["json"]
        if "categories" in fields[key]:
            years = fields[key]["categories"]["json"]

    assert all([len(elements[country]) == len(years) for country in elements])
    # Use years as index and elements (data for each country) as columns.
    df = pd.DataFrame(elements, index=years)

    # Rearrange dataframe for convenience.
    df = df.reset_index().rename(columns={"index": "year"}).astype({"year": int})

    return df


def fetch_all_data_for_energy_source(energy_source: str) -> pd.DataFrame:
    """Fetch all data for a specific energy source and all countries.

    The list of countries is defined above, in SHIFT_COUNTRIES.

    Parameters
    ----------
    energy_source : str
        Name of energy source (e.g. "coal").

    Returns
    -------
    combined : pd.DataFrame
        Data for a specific energy source and all countries.

    """
    # Split list of countries in smaller chunks to avoid errors when requesting data.
    n_chunks = int(len(SHIFT_COUNTRIES) / MAX_NUM_COUNTRIES_PER_QUERY) + 1
    # Create chunks of country names.
    countries_chunks = np.array_split(SHIFT_COUNTRIES, n_chunks)
    dfs = []
    for countries_chunk in tqdm(countries_chunks, desc="Subset of countries"):
        # Fetch data for current chunk of countries and specified energy source.
        df = fetch_data_for_energy_source_and_a_list_of_countries(
            energy_source=energy_source, countries=countries_chunk
        )
        # Wait between consecutive requests.
        sleep(TIME_BETWEEN_QUERIES)
        # Collect data for current chunk of countries.
        dfs.append(df)

    # Combine dataframes of all chunks of countries into one dataframe.
    combined = dataframes.multi_merge(dfs=dfs, on="year", how="inner")
    # Restructure dataframe conveniently.
    combined = combined.melt(
        id_vars="year", value_name=energy_source, var_name="country"
    )
    combined = combined.sort_values(["country", "year"]).reset_index(drop=True)

    return combined


def fetch_all_data_for_all_energy_sources() -> pd.DataFrame:
    """Fetch all Shift data for all energy sources and all countries.

    The list of energy sources and countries are defined above, in ENERGY_SOURCES and SHIFT_COUNTRIES, respectively.

    Returns
    -------
    energy_data : pd.DataFrame
        Energy data for all energy sources and countries specified above.

    """
    energy_dfs = []
    for energy_source in tqdm(ENERGY_SOURCES, desc="Energy source"):
        # Fetch all data for current energy source.
        energy_df = fetch_all_data_for_energy_source(energy_source=energy_source)
        energy_dfs.append(energy_df)

    # Combine data from different energy sources.
    energy_data = dataframes.multi_merge(
        energy_dfs, on=["country", "year"], how="outer"
    )

    # Create index.
    energy_data = energy_data.set_index(
        ["country", "year"], verify_integrity=True
    ).sort_index()

    return energy_data


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Walden",
)
def main(upload: bool) -> None:
    # Download all data from Shift as a dataframe.
    energy_data = fetch_all_data_for_all_energy_sources()

    # Load metadata from the corresponding yaml file.
    metadata = Dataset.from_yaml(METADATA_FILE)

    # TODO: Fix possible bug: metadata.version seems to be datetime.date, which makes add_to_catalog fail.
    #  I convert it to string.
    metadata.version = metadata.version.isoformat()

    with tempfile.NamedTemporaryFile() as _temp_file:
        # Save data in a temporary feather file.
        energy_data.to_csv(_temp_file.name)
        # Add file checksum to metadata.
        metadata.md5 = files.checksum(_temp_file.name)
        # Create walden index file and upload to s3 (if upload is True).
        add_to_catalog(metadata, _temp_file.name, upload=upload)


if __name__ == "__main__":
    main()
