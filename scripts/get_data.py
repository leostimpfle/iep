from duckdb import DuckDBPyRelation

import iep


def filter(data: DuckDBPyRelation, facility: str) -> DuckDBPyRelation:
    identifiers = iep.identifiers.load()
    return data.join(
        identifiers.filter(f"Facility_INSPIRE_ID = '{facility}'").select(
            "Installation_Part_INSPIRE_ID"
        ),
        condition="Installation_Part_INSPIRE_ID",
        how="inner",
    )


facility = r"AT.CAED/9008390661741.FACILITY"
parts = filter(iep.part.part.load(reload=False), facility=facility)
details = filter(iep.part.details.load(), facility=facility)
desulphurisation = filter(iep.part.desulphurisation.load(), facility=facility)
derogations = filter(iep.part.derogations.load(), facility=facility)
emissions = filter(iep.part.emissions.load(), facility=facility)
energy_input = filter(iep.part.energyinput.load(), facility=facility)


# %%
fids = (
    "PL.MŚ/000003639.FACILITY",
    r"PL.MŚ/000003640.FACILITY",
    r"PL.MŚ/000000114.FACILITY",
)
facilities = iep.facility.facility.load()
print(facilities.filter(f"Facility_INSPIRE_ID = '{facility}'").select("nameOfFeature"))
pollutants = iep.facility.pollutant_release.load(
    deduplicate=True, sanitise=True, interpolate=True
)
pollutants.filter(
    f"Facility_INSPIRE_ID IN {fids} AND pollutantCode = 'CO2' AND medium = 'AIR'"
).select("reportingYear, totalPollutantQuantityKg").order("reportingYear")
