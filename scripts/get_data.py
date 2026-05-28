import altair
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


identifiers = iep.identifiers.load()
energy_inputs = iep.part.energy_input.load(sanitise=True)

fid = "LT.CAED/153009143.FACILITY"
fid = "DK.CAED/000082948.FACILITY"
fid = "FR.CAED/13773.FACILITY"
fid = "IT.CAED/660503069.FACILITY"
fid = "IT.CAED/660502007.FACILITY"
energy_inputs.join(
    identifiers.filter(f"Facility_INSPIRE_ID = '{fid}'").select(
        "Installation_Part_INSPIRE_ID"
    ),
    condition="Installation_Part_INSPIRE_ID",
    how="inner",
).filter("energyInputTJ > 0.0").aggregate(
    "reportingYear, fuelInputCode, SUM(energyInputTJ)"
).order("reportingYear")

facility = iep.facility.facility._load_raw()
pollutants = iep.facility.pollutant_release.load(sanitise=False)
part = "ES.CAED/002112001.PART"
facility = (
    identifiers.filter(f"Installation_Part_INSPIRE_ID = '{part}'")
    .select("Facility_INSPIRE_ID")
    .distinct()
)
pollutants.join(facility, condition="Facility_INSPIRE_ID", how="inner").filter(
    "pollutantCode = 'CO2' AND medium = 'AIR'"
).select("reportingYear, totalPollutantQuantityKg").order("reportingYear")


tmp = (
    pollutants.join(
        facility.filter("countryCode = 'IS'").select("Facility_INSPIRE_ID"),
        condition="Facility_INSPIRE_ID",
        how="inner",
    )
    .filter("pollutantCode = 'CO2' AND medium = 'AIR'")
    .select(
        "*, totalPollutantQuantityKg - LAG(totalPollutantQuantityKg) OVER (PARTITION BY Facility_INSPIRE_ID ORDER BY reportingYear) AS d"
    )
    .aggregate("ARGMIN(Facility_INSPIRE_ID, d)")
    # .aggregate(
    #     "reportingYear, SUM(totalPollutantQuantityKg) AS totalPollutantQuantityKg"
    # )
)
altair.Chart(tmp).mark_line().encode(
    x="reportingYear:O", y="totalPollutantQuantityKg:Q"
).properties(width=800, height=800).save(
    r"/Users/leonardstimpfle/Downloads/iceland.html"
)
# facility = r"AT.CAED/9008390661741.FACILITY"
# parts = filter(iep.part.part.load(reload=False), facility=facility)
# details = filter(iep.part.details.load(), facility=facility)
# desulphurisation = filter(iep.part.desulphurisation.load(), facility=facility)
# derogations = filter(iep.part.derogations.load(), facility=facility)
# emissions = filter(iep.part.emissions.load(), facility=facility)
# energy_input = filter(iep.part.energyinput.load(), facility=facility)
#
#
# # %%
# fids = (
#     "PL.MŚ/000003639.FACILITY",
#     r"PL.MŚ/000003640.FACILITY",
#     r"PL.MŚ/000000114.FACILITY",
# )
# facilities = iep.facility.facility.load()
# print(facilities.filter(f"Facility_INSPIRE_ID = '{facility}'").select("nameOfFeature"))
# pollutants = iep.facility.pollutant_release.load(
#     deduplicate=True, sanitise=True, interpolate=True
# )
# pollutants.filter(
#     f"Facility_INSPIRE_ID IN {fids} AND pollutantCode = 'CO2' AND medium = 'AIR'"
# ).select("reportingYear, totalPollutantQuantityKg").order("reportingYear")
